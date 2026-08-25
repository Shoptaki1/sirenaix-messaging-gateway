package domain

import (
	"fmt"
	"strings"
	"unicode"
)

// E164Phone is a canonical international number. Values can only be created by
// ParseE164, which never guesses a country code for local numbers.
type E164Phone struct {
	value string
}

func ParseE164(input string) (E164Phone, error) {
	input = strings.TrimSpace(input)
	if len(input) < 2 || input[0] != '+' {
		return E164Phone{}, fmt.Errorf("%w: %q", ErrInvalidPhoneNumber, input)
	}

	digits := make([]byte, 0, len(input)-1)
	for i := 1; i < len(input); i++ {
		char := input[i]
		switch {
		case char >= '0' && char <= '9':
			digits = append(digits, char)
		case char == ' ' || char == '\t' || char == '(' || char == ')' || char == '-' || char == '.':
			continue
		default:
			return E164Phone{}, fmt.Errorf("%w: %q", ErrInvalidPhoneNumber, input)
		}
	}
	if len(digits) < 2 || len(digits) > 15 || digits[0] == '0' {
		return E164Phone{}, fmt.Errorf("%w: %q", ErrInvalidPhoneNumber, input)
	}
	return E164Phone{value: "+" + string(digits)}, nil
}

func (phone E164Phone) String() string {
	return phone.value
}

type Contact struct {
	ID                  ContactID
	TenantID            TenantID
	Phone               E164Phone
	Alias               string
	ProviderDisplayName string
	LabelIDs            []LabelID
}

func (contact Contact) EffectiveDisplayName() string {
	if alias := strings.TrimSpace(contact.Alias); alias != "" {
		return alias
	}
	if providerName := strings.TrimSpace(contact.ProviderDisplayName); providerName != "" {
		return providerName
	}
	return contact.Phone.String()
}

// ProviderContactSource identifies one provider address-book record. A contact
// may have several sources, including multiple sources on the same connection.
type ProviderContactSource struct {
	TenantID            TenantID
	ConnectionID        ConnectionID
	ProviderContactID   string
	ContactID           ContactID
	ProviderDisplayName string
}

type Label struct {
	ID       LabelID
	TenantID TenantID
	Name     string
	Slug     string
}

func NewLabel(id LabelID, tenantID TenantID, name string) (Label, error) {
	if tenantID == "" {
		return Label{}, ErrInvalidTenantID
	}
	if id == "" {
		return Label{}, ErrInvalidIdentifier
	}
	slug := normalizeSlug(name)
	if slug == "" {
		return Label{}, ErrInvalidLabelSlug
	}
	return Label{ID: id, TenantID: tenantID, Name: strings.TrimSpace(name), Slug: slug}, nil
}

func normalizeSlug(input string) string {
	var slug strings.Builder
	separatorPending := false
	for _, char := range strings.TrimSpace(input) {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			if separatorPending && slug.Len() > 0 {
				slug.WriteByte('-')
			}
			slug.WriteRune(unicode.ToLower(char))
			separatorPending = false
		} else {
			separatorPending = true
		}
	}
	return slug.String()
}
