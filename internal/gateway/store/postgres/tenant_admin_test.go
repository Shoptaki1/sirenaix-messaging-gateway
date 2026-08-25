package postgres

import (
	"errors"
	"testing"
)

func TestTenantAdminInputRejectsAmbiguousIdentifiersNamesAndQuotas(t *testing.T) {
	valid := TenantAdminInput{TenantID: "tenant-west_2", Name: "West Dental", MaxConnections: 24, Actor: "deploy/operator"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid input error = %v", err)
	}
	for _, input := range []TenantAdminInput{
		{TenantID: "", Name: "West", MaxConnections: 1, Actor: "operator"},
		{TenantID: "Tenant Upper", Name: "West", MaxConnections: 1, Actor: "operator"},
		{TenantID: "tenant", Name: "", MaxConnections: 1, Actor: "operator"},
		{TenantID: "tenant", Name: " West", MaxConnections: 1, Actor: "operator"},
		{TenantID: "tenant", Name: "West", MaxConnections: 0, Actor: "operator"},
		{TenantID: "tenant", Name: "West", MaxConnections: 129, Actor: "operator"},
		{TenantID: "tenant", Name: "West", MaxConnections: 1, Actor: "line\nbreak"},
	} {
		if err := input.Validate(); !errors.Is(err, ErrInvalidTenantAdminInput) {
			t.Fatalf("input %+v error = %v, want ErrInvalidTenantAdminInput", input, err)
		}
	}
}

func TestTenantAdminActionsAreClosedEnumeration(t *testing.T) {
	for _, action := range []TenantAdminAction{TenantActionProvision, TenantActionStatus, TenantActionSuspend, TenantActionResume} {
		if err := action.Validate(); err != nil {
			t.Fatalf("action %q error = %v", action, err)
		}
	}
	if err := TenantAdminAction("delete").Validate(); !errors.Is(err, ErrInvalidTenantAdminInput) {
		t.Fatalf("delete action error = %v, want ErrInvalidTenantAdminInput", err)
	}
}
