package googleworkspace

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	"google.golang.org/api/googleapi"
)

//// TABLE DEFINITION

func tableGoogleWorkspaceTokensList(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "googleworkspace_tokens_list",
		Description: "Retrieve OAuth 2.0 tokens issued to 3rd-party applications for all users in the Google Workspace directory.",
		List: &plugin.ListConfig{
			Hydrate: listAllTokens,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.AllColumns([]string{"user_key", "client_id"}),
			Hydrate:    getToken,
		},
		Columns: []*plugin.Column{
			{
				Name:        "client_id",
				Description: "The unique ID of the application the token is issued to.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ClientId"),
			},
			{
				Name:        "user_key",
				Description: "The user email associated with the token.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("UserKey"),
			},
			{
				Name:        "primary_email",
				Description: "The primary email of the user who authorized the token.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("PrimaryEmail"),
			},
			{
				Name:        "scopes",
				Description: "The list of scopes granted to the application.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "anonymous",
				Description: "Indicates if the token is issued to an anonymous user.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "display_text",
				Description: "The display text for the application.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("DisplayText"),
			},
			{
				Name:        "native_app",
				Description: "Indicates if the token is issued to a native application.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("NativeApp"),
			},
			{
				Name:        "kind",
				Description: "The type of the API resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "etag",
				Description: "The ETag of the resource.",
				Type:        proto.ColumnType_STRING,
			},
		},
	}
}

// TokenWithUser combines token data with user information
type TokenWithUser struct {
	UserKey      string   `json:"user_key"`
	PrimaryEmail string   `json:"primary_email"`
	ClientId     string   `json:"client_id"`
	Scopes       []string `json:"scopes"`
	Anonymous    bool     `json:"anonymous"`
	DisplayText  string   `json:"display_text"`
	NativeApp    bool     `json:"native_app"`
	Kind         string   `json:"kind"`
	Etag         string   `json:"etag"`
}

//// LIST FUNCTION

func listAllTokens(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	// First, get all users
	userFields := googleapi.Field("users(id,primaryEmail)")
	usersReq := service.Users.List().Customer("my_customer").Fields(userFields).MaxResults(500)

	usersResp, err := usersReq.Do()
	if err != nil {
		return nil, err
	}

	// Then, for each user, get their tokens
	tokenFields := googleapi.Field("items(clientId,scopes,anonymous,displayText,nativeApp,kind,etag)")

	for _, user := range usersResp.Users {
		// Check if we should continue processing
		if d.RowsRemaining(ctx) == 0 {
			break
		}

		tokensReq := service.Tokens.List(user.PrimaryEmail).Fields(tokenFields)
		tokensResp, err := tokensReq.Do()
		if err != nil {
			// Skip users who don't have tokens or access denied
			continue
		}

		for _, token := range tokensResp.Items {
			tokenWithUser := &TokenWithUser{
				UserKey:      user.PrimaryEmail,
				PrimaryEmail: user.PrimaryEmail,
				ClientId:     token.ClientId,
				Scopes:       token.Scopes,
				Anonymous:    token.Anonymous,
				DisplayText:  token.DisplayText,
				NativeApp:    token.NativeApp,
				Kind:         token.Kind,
				Etag:         token.Etag,
			}

			d.StreamListItem(ctx, tokenWithUser)

			if d.RowsRemaining(ctx) == 0 {
				break
			}
		}
	}

	return nil, nil
}

//// GET FUNCTION

func getToken(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	userKey := d.EqualsQualString("user_key")
	clientId := d.EqualsQualString("client_id")

	if userKey == "" || clientId == "" {
		return nil, nil
	}

	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	fields := googleapi.Field("clientId,scopes,anonymous,displayText,nativeApp,kind,etag")

	token, err := service.Tokens.Get(userKey, clientId).Fields(fields).Do()
	if err != nil {
		return nil, err
	}

	// Get user info for the response
	userFields := googleapi.Field("primaryEmail")
	user, err := service.Users.Get(userKey).Fields(userFields).Do()
	if err != nil {
		return nil, err
	}

	tokenWithUser := &TokenWithUser{
		UserKey:      userKey,
		PrimaryEmail: user.PrimaryEmail,
		ClientId:     token.ClientId,
		Scopes:       token.Scopes,
		Anonymous:    token.Anonymous,
		DisplayText:  token.DisplayText,
		NativeApp:    token.NativeApp,
		Kind:         token.Kind,
		Etag:         token.Etag,
	}

	return tokenWithUser, nil
}
