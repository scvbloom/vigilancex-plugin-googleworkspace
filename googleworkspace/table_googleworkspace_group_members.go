package googleworkspace

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/googleapi"
)

//// TABLE DEFINITION

func tableGoogleWorkspaceGroupMembers(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "googleworkspace_group_members",
		Description: "Retrieve members of groups in the Google Workspace directory.",
		List: &plugin.ListConfig{
			// Remove KeyColumns requirement to allow querying all groups
			Hydrate: listAllGroupMembers,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"group_key", "member_key"}),
			Hydrate:    getGroupMember,
		},
		Columns: []*plugin.Column{
			{
				Name:        "group_key",
				Description: "The unique identifier of the group (email or ID).",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("group_key"),
			},
			{
				Name:        "member_key",
				Description: "The member's email address or unique ID.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Email"),
			},
			{
				Name:        "id",
				Description: "The unique ID of the member.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "email",
				Description: "The member's email address.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "role",
				Description: "The member's role in the group (OWNER, MANAGER, MEMBER).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "type",
				Description: "The type of member (USER, GROUP, CUSTOMER).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "status",
				Description: "The member's status in the group.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "delivery_settings",
				Description: "The member's delivery settings for group emails.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("DeliverySettings"),
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

// MemberWithGroup combines member data with group information
type MemberWithGroup struct {
	GroupKey         string `json:"group_key"`
	Id               string `json:"id"`
	Email            string `json:"email"`
	Role             string `json:"role"`
	Type             string `json:"type"`
	Status           string `json:"status"`
	DeliverySettings string `json:"delivery_settings"`
	Etag             string `json:"etag"`
	Kind             string `json:"kind"`
}

//// LIST FUNCTION

func listGroupMembers(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	groupKey := d.EqualsQualString("group_key")
	if groupKey == "" {
		return nil, nil
	}

	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	// Specify fields to retrieve
	fields := googleapi.Field("members(id,email,role,type,status,delivery_settings,etag,kind)")

	req := service.Members.List(groupKey).Fields(fields).MaxResults(200)

	for {
		resp, err := req.Do()
		if err != nil {
			return nil, err
		}

		for _, member := range resp.Members {
			memberWithGroup := &MemberWithGroup{
				GroupKey:         groupKey,
				Id:               member.Id,
				Email:            member.Email,
				Role:             member.Role,
				Type:             member.Type,
				Status:           member.Status,
				DeliverySettings: member.DeliverySettings,
				Etag:             member.Etag,
				Kind:             member.Kind,
			}

			d.StreamListItem(ctx, memberWithGroup)

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

func getGroupMember(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	groupKey := d.EqualsQualString("group_key")
	memberKey := d.EqualsQualString("member_key")

	if groupKey == "" || memberKey == "" {
		return nil, nil
	}

	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	// Specify fields to retrieve
	fields := googleapi.Field("id,email,role,type,status,delivery_settings,etag,kind")

	member, err := service.Members.Get(groupKey, memberKey).Fields(fields).Do()
	if err != nil {
		return nil, err
	}

	memberWithGroup := &MemberWithGroup{
		GroupKey:         groupKey,
		Id:               member.Id,
		Email:            member.Email,
		Role:             member.Role,
		Type:             member.Type,
		Status:           member.Status,
		DeliverySettings: member.DeliverySettings,
		Etag:             member.Etag,
		Kind:             member.Kind,
	}

	return memberWithGroup, nil
}

// New function to list all group members across all groups
func listAllGroupMembers(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Check if group_key is specified
	groupKey := d.EqualsQualString("group_key")
	if groupKey != "" {
		// If group_key is specified, use the existing function logic
		return listGroupMembers(ctx, d, nil)
	}

	// If no group_key specified, iterate through all groups
	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	// First get all groups
	groupsReq := service.Groups.List().Customer("my_customer").Fields("groups(id,email)")
	groupsResp, err := groupsReq.Do()
	if err != nil {
		return nil, err
	}

	// Then get members for each group
	memberFields := googleapi.Field("members(id,email,role,type,status,delivery_settings,etag,kind)")

	for _, group := range groupsResp.Groups {
		if d.RowsRemaining(ctx) == 0 {
			break
		}

		membersReq := service.Members.List(group.Email).Fields(memberFields).MaxResults(200)
		membersResp, err := membersReq.Do()
		if err != nil {
			// Skip groups without members or access denied
			continue
		}

		for _, member := range membersResp.Members {
			memberWithGroup := &MemberWithGroup{
				GroupKey:         group.Email,
				Id:               member.Id,
				Email:            member.Email,
				Role:             member.Role,
				Type:             member.Type,
				Status:           member.Status,
				DeliverySettings: member.DeliverySettings,
				Etag:             member.Etag,
				Kind:             member.Kind,
			}

			d.StreamListItem(ctx, memberWithGroup)

			if d.RowsRemaining(ctx) == 0 {
				break
			}
		}
	}

	return nil, nil
}
