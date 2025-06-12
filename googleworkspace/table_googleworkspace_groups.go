package googleworkspace

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/googleapi"
)

//// TABLE DEFINITION

func tableGoogleWorkspaceGroups(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "googleworkspace_groups",
		Description: "Retrieve groups in the Google Workspace directory.",
		List: &plugin.ListConfig{
			Hydrate: listGroups,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("id"),
			Hydrate:    getGroup,
		},
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Description: "The unique ID of the group.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "email",
				Description: "The email address of the group.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "name",
				Description: "The display name of the group.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "description",
				Description: "The description of the group.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "direct_members_count",
				Description: "The number of direct members in the group.",
				Type:        proto.ColumnType_INT,
				Transform:   transform.FromField("DirectMembersCount"),
			},
			{
				Name:        "admin_created",
				Description: "Indicates if the group was created by an admin.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("AdminCreated"),
			},
			{
				Name:        "aliases",
				Description: "The list of aliases for the group.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "non_editable_aliases",
				Description: "The list of non-editable aliases for the group.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("NonEditableAliases"),
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
		},
	}
}

//// LIST FUNCTION

func listGroups(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	// Specify fields to retrieve
	fields := googleapi.Field("groups(id,email,name,description,directMembersCount,adminCreated,aliases,nonEditableAliases,etag,kind)")

	req := service.Groups.List().Customer("my_customer").Fields(fields).MaxResults(200)

	for {
		resp, err := req.Do()
		if err != nil {
			return nil, err
		}

		for _, group := range resp.Groups {
			d.StreamListItem(ctx, group)

			// Check if we should continue processing
			if d.RowsRemaining(ctx) == 0 {
				return nil, nil
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		req.PageToken(resp.NextPageToken)
	}

	return nil, nil
}

//// GET FUNCTION

func getGroup(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	groupId := d.EqualsQualString("id")

	if groupId == "" {
		return nil, nil
	}

	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	// Specify fields to retrieve
	fields := googleapi.Field("id,email,name,description,directMembersCount,adminCreated,aliases,nonEditableAliases,etag,kind")

	group, err := service.Groups.Get(groupId).Fields(fields).Do()
	if err != nil {
		return nil, err
	}

	return group, nil
}
