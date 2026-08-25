package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"go.mau.fi/mautrix-gmessages/internal/gateway/contacts"
	"go.mau.fi/mautrix-gmessages/internal/gateway/contactsync"
	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

var _ contactsync.ConnectionRepository = (*Repository)(nil)
var _ contactsync.ContactRepository = (*Repository)(nil)
var _ contacts.Store = (*Repository)(nil)

const upsertServerContactSQL = `/* op:upsert_server_contact */
INSERT INTO contacts (
    tenant_id, contact_id, normalized_phone, server_alias
) VALUES ($1, $2, $3, CASE WHEN $5 THEN $4 ELSE '' END)
ON CONFLICT (tenant_id, normalized_phone) DO UPDATE
SET server_alias = CASE WHEN $5 THEN EXCLUDED.server_alias ELSE contacts.server_alias END,
    updated_at = CASE WHEN $5 THEN now() ELSE contacts.updated_at END
RETURNING contact_id, normalized_phone, server_alias, provider_display_name,
          COALESCE((
              SELECT json_agg(contact_labels.label_id ORDER BY contact_labels.label_id)::text
              FROM contact_labels
              WHERE contact_labels.tenant_id = contacts.tenant_id
                AND contact_labels.contact_id = contacts.contact_id
          ), '[]')`

// UpsertServerContact creates a tenant-local contact before Google has exposed
// it, or converges on the existing canonical phone. A nil alias means
// "preserve"; a non-nil empty alias explicitly clears it. Provider metadata
// and label links are never changed by this operation.
func (repository *Repository) UpsertServerContact(ctx context.Context, tenantID domain.TenantID, createdID domain.ContactID, phone domain.E164Phone, alias *string) (domain.Contact, error) {
	createdIDText := string(createdID)
	if tenantID == "" || createdID == "" || len(createdID) > 256 || !utf8.ValidString(createdIDText) ||
		strings.TrimSpace(createdIDText) != createdIDText || strings.ContainsAny(createdIDText, "\x00\r\n") || phone.String() == "" {
		return domain.Contact{}, contacts.ErrInvalidContact
	}
	aliasValue, aliasProvided := "", alias != nil
	if alias != nil {
		aliasValue = strings.TrimSpace(*alias)
		if len(aliasValue) > contacts.MaxServerAliasBytes || !utf8.ValidString(aliasValue) || strings.ContainsRune(aliasValue, '\x00') {
			return domain.Contact{}, contacts.ErrInvalidContact
		}
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (domain.Contact, error) {
		contact, err := scanContact(tx.QueryRowContext(ctx, upsertServerContactSQL,
			string(tenantID), string(createdID), phone.String(), aliasValue, aliasProvided,
		), tenantID)
		if err != nil {
			return domain.Contact{}, fmt.Errorf("upsert server contact: %w", err)
		}
		return contact, nil
	})
}

const upsertProviderContactSQL = `/* op:upsert_provider_contact */
WITH canonical AS (
    INSERT INTO contacts (
        tenant_id, contact_id, normalized_phone, provider_display_name
    ) VALUES ($1, $5, $4, $6)
    ON CONFLICT (tenant_id, normalized_phone) DO UPDATE
    SET provider_display_name = EXCLUDED.provider_display_name,
        updated_at = now()
    RETURNING tenant_id, contact_id, normalized_phone, server_alias, provider_display_name
), source AS (
    INSERT INTO provider_contact_sources (
        tenant_id, connection_id, provider_contact_id, contact_id,
        normalized_phone, provider_display_name
    )
    SELECT $1, $2, $3, canonical.contact_id,
           canonical.normalized_phone, $6
    FROM canonical
    ON CONFLICT (tenant_id, connection_id, provider_contact_id) DO UPDATE
    SET provider_display_name = EXCLUDED.provider_display_name,
        updated_at = now()
    WHERE provider_contact_sources.contact_id = EXCLUDED.contact_id
      AND provider_contact_sources.normalized_phone = EXCLUDED.normalized_phone
    RETURNING contact_id
)
SELECT canonical.contact_id, canonical.normalized_phone, canonical.server_alias,
       canonical.provider_display_name,
       COALESCE((
           SELECT json_agg(contact_labels.label_id ORDER BY contact_labels.label_id)::text
           FROM contact_labels
           WHERE contact_labels.tenant_id = $1
             AND contact_labels.contact_id = canonical.contact_id
       ), '[]')
FROM canonical
JOIN source ON source.contact_id = canonical.contact_id`

func (repository *Repository) UpsertProviderContact(ctx context.Context, update contactsync.ProviderContactUpsert) (domain.Contact, error) {
	if update.TenantID == "" {
		return domain.Contact{}, domain.ErrInvalidTenantID
	}
	if update.ConnectionID == "" || strings.TrimSpace(update.ProviderContactID) == "" || update.Phone.String() == "" {
		return domain.Contact{}, domain.ErrInvalidIdentifier
	}
	return inTenant(ctx, repository, update.TenantID, func(tx transaction) (domain.Contact, error) {
		contact, err := scanContact(tx.QueryRowContext(ctx, upsertProviderContactSQL,
			string(update.TenantID), string(update.ConnectionID), update.ProviderContactID,
			update.Phone.String(), repository.newID(), strings.TrimSpace(update.ProviderDisplayName),
		), update.TenantID)
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Contact{}, contactsync.ErrProviderIdentityConflict
		}
		if err != nil {
			return domain.Contact{}, fmt.Errorf("upsert provider contact: %w", err)
		}
		return contact, nil
	})
}

const listContactsSQL = `/* op:list_contacts */
SELECT contacts.contact_id, contacts.normalized_phone, contacts.server_alias,
       contacts.provider_display_name,
       COALESCE((
           SELECT json_agg(contact_labels.label_id ORDER BY contact_labels.label_id)::text
           FROM contact_labels
           WHERE contact_labels.tenant_id = contacts.tenant_id
             AND contact_labels.contact_id = contacts.contact_id
       ), '[]')
FROM contacts
WHERE contacts.tenant_id = $1 AND contacts.contact_id > $2
ORDER BY contacts.contact_id
LIMIT $3`

func (repository *Repository) ListContacts(ctx context.Context, tenantID domain.TenantID, options ContactListOptions) (ContactPage, error) {
	if options.Limit < 0 || options.Limit > 200 {
		return ContactPage{}, ErrInvalidCursor
	}
	limit := options.Limit
	if limit == 0 {
		limit = 50
	}
	return inTenant(ctx, repository, tenantID, func(tx transaction) (ContactPage, error) {
		rows, err := tx.QueryContext(ctx, listContactsSQL, string(tenantID), string(options.After), limit+1)
		if err != nil {
			return ContactPage{}, fmt.Errorf("list contacts: %w", err)
		}
		defer rows.Close()
		page := ContactPage{}
		for rows.Next() {
			contact, err := scanContact(rows, tenantID)
			if err != nil {
				return ContactPage{}, fmt.Errorf("scan contact: %w", err)
			}
			page.Contacts = append(page.Contacts, contact)
		}
		if err := rows.Err(); err != nil {
			return ContactPage{}, fmt.Errorf("iterate contacts: %w", err)
		}
		if len(page.Contacts) > limit {
			page.Contacts = page.Contacts[:limit]
			page.NextCursor = page.Contacts[len(page.Contacts)-1].ID
		}
		return page, nil
	})
}

func scanContact(row rowScanner, tenantID domain.TenantID) (domain.Contact, error) {
	var id, number, alias, providerName, labelsJSON string
	if err := row.Scan(&id, &number, &alias, &providerName, &labelsJSON); err != nil {
		return domain.Contact{}, err
	}
	phone, err := domain.ParseE164(number)
	if err != nil {
		return domain.Contact{}, fmt.Errorf("stored contact phone: %w", err)
	}
	var rawLabelIDs []string
	if err := json.Unmarshal([]byte(labelsJSON), &rawLabelIDs); err != nil {
		return domain.Contact{}, fmt.Errorf("decode contact labels: %w", err)
	}
	labelIDs := make([]domain.LabelID, len(rawLabelIDs))
	for index, labelID := range rawLabelIDs {
		labelIDs[index] = domain.LabelID(labelID)
	}
	return domain.Contact{
		ID: domain.ContactID(id), TenantID: tenantID, Phone: phone,
		Alias: alias, ProviderDisplayName: providerName, LabelIDs: labelIDs,
	}, nil
}

const setContactAliasSQL = `/* op:set_contact_alias */
UPDATE contacts
SET server_alias = $3, updated_at = now()
WHERE tenant_id = $1 AND contact_id = $2`

func (repository *Repository) SetContactAlias(ctx context.Context, tenantID domain.TenantID, contactID domain.ContactID, alias string) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if contactID == "" {
		return domain.ErrInvalidIdentifier
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		result, err := tx.ExecContext(ctx, setContactAliasSQL, string(tenantID), string(contactID), strings.TrimSpace(alias))
		if err != nil {
			return fmt.Errorf("set contact alias: %w", err)
		}
		return requireAffected(result, ErrContactNotFound)
	})
}

const clearContactAliasSQL = `/* op:clear_contact_alias */
UPDATE contacts
SET server_alias = '', updated_at = now()
WHERE tenant_id = $1 AND contact_id = $2`

func (repository *Repository) ClearContactAlias(ctx context.Context, tenantID domain.TenantID, contactID domain.ContactID) error {
	if tenantID == "" {
		return domain.ErrInvalidTenantID
	}
	if contactID == "" {
		return domain.ErrInvalidIdentifier
	}
	return inTenantExec(ctx, repository, tenantID, func(tx transaction) error {
		result, err := tx.ExecContext(ctx, clearContactAliasSQL, string(tenantID), string(contactID))
		if err != nil {
			return fmt.Errorf("clear contact alias: %w", err)
		}
		return requireAffected(result, ErrContactNotFound)
	})
}
