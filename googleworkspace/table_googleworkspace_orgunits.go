package googleworkspace

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/googleapi"
)

//// TABLE DEFINITION

func tableGoogleWorkspaceOrgUnits(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "googleworkspace_orgunits",
		Description: "Retrieve organizational units for a specific customer in the Google Workspace directory.",
		List: &plugin.ListConfig{
			Hydrate: listOrgUnits,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("org_unit_path"),
			Hydrate:    getOrgUnit,
		},
		Columns: []*plugin.Column{
			{
				Name:        "org_unit_id",
				Description: "The unique ID of the organizational unit.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("OrgUnitId"),
			},
			{
				Name:        "name",
				Description: "The name of the organizational unit.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "description",
				Description: "The description of the organizational unit.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "org_unit_path",
				Description: "The full path to the organizational unit.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("OrgUnitPath"),
			},
			{
				Name:        "parent_org_unit_id",
				Description: "The unique ID of the parent organizational unit.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ParentOrgUnitId"),
			},
			{
				Name:        "parent_org_unit_path",
				Description: "The full path to the parent organizational unit.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ParentOrgUnitPath"),
			},
			{
				Name:        "block_inheritance",
				Description: "Indicates if the organizational unit blocks policy inheritance.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("BlockInheritance"),
			},
			{
				Name:        "etag",
				Description: "The ETag of the resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "kind",
				Description: "The type of the API resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "customer_id",
				Description: "The customer ID that owns the organizational unit.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("customer_id"),
			},
		},
	}
}

//// LIST FUNCTION

func listOrgUnits(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	customerId := "my_customer"
	if d.EqualsQualString("customer_id") != "" {
		customerId = d.EqualsQualString("customer_id")
	}

	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	fields := googleapi.Field("organizationUnits(orgUnitId,name,description,orgUnitPath,parentOrgUnitId,parentOrgUnitPath,blockInheritance,etag,kind)")

	req := service.Orgunits.List(customerId).Fields(fields)

	if d.EqualsQualString("org_unit_path") != "" {
		req = req.OrgUnitPath(d.EqualsQualString("org_unit_path"))
	}

	if d.EqualsQualString("type") != "" {
		req = req.Type(d.EqualsQualString("type"))
	}

	resp, err := req.Do()
	if err != nil {
		return nil, err
	}

	for _, orgUnit := range resp.OrganizationUnits {
		d.StreamListItem(ctx, orgUnit)

		if d.RowsRemaining(ctx) == 0 {
			break
		}
	}

	return nil, nil
}

//// GET FUNCTION

func getOrgUnit(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	customerId := "my_customer"
	if d.EqualsQualString("customer_id") != "" {
		customerId = d.EqualsQualString("customer_id")
	}

	orgUnitPath := d.EqualsQualString("org_unit_path")
	if orgUnitPath == "" {
		return nil, nil
	}

	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	fields := googleapi.Field("orgUnitId,name,description,orgUnitPath,parentOrgUnitId,parentOrgUnitPath,blockInheritance,etag,kind")

	resp, err := service.Orgunits.Get(customerId, orgUnitPath).Fields(fields).Do()
	if err != nil {
		return nil, err
	}

	return resp, nil
}
