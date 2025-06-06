package googleworkspace

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
	admin "google.golang.org/api/admin/directory/v1"
	"google.golang.org/api/googleapi"
)

//// TABLE DEFINITION

func tableGoogleWorkspaceDirectoryUsers(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "googleworkspace_directory_users",
		Description: "Retrieve information about users in the Google Workspace directory.",
		List: &plugin.ListConfig{
			Hydrate: listDirectoryUsers,
		},
		Columns: []*plugin.Column{
			{
				Name:        "id",
				Description: "The unique ID for the user.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "primary_email",
				Description: "The user's primary email address.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("PrimaryEmail"),
			},
			{
				Name:        "name",
				Description: "The user's name details.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "given_name",
				Description: "The user's first name.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name.GivenName"),
			},
			{
				Name:        "family_name",
				Description: "The user's last name.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name.FamilyName"),
			},
			{
				Name:        "full_name",
				Description: "The user's full name.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name.FullName"),
			},
			{
				Name:        "is_admin",
				Description: "Indicates if the user is an administrator.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("IsAdmin"),
			},
			{
				Name:        "is_delegated_admin",
				Description: "Indicates if the user is a delegated administrator.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("IsDelegatedAdmin"),
			},
			{
				Name:        "is_suspended",
				Description: "Indicates if the user is suspended.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("Suspended"),
			},
			{
				Name:        "suspension_reason",
				Description: "The reason for the user's suspension.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("SuspensionReason"),
			},
			{
				Name:        "archived",
				Description: "Indicates if the user is archived.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "agreed_to_terms",
				Description: "Indicates if the user has agreed to terms.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("AgreedToTerms"),
			},
			{
				Name:        "change_password_at_next_login",
				Description: "Indicates if the user must change password at next login.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("ChangePasswordAtNextLogin"),
			},
			{
				Name:        "include_in_global_address_list",
				Description: "Indicates if the user is included in the global address list.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("IncludeInGlobalAddressList"),
			},
			{
				Name:        "ip_whitelisted",
				Description: "Indicates if the user's IP is whitelisted.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("IpWhitelisted"),
			},
			{
				Name:        "is_mailbox_setup",
				Description: "Indicates if the user's mailbox is set up.",
				Type:        proto.ColumnType_BOOL,
				Transform:   transform.FromField("IsMailboxSetup"),
			},
			{
				Name:        "last_login_time",
				Description: "The last time the user logged in.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("LastLoginTime").NullIfZero(),
			},
			{
				Name:        "creation_time",
				Description: "The time the user was created.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("CreationTime").NullIfZero(),
			},
			{
				Name:        "deletion_time",
				Description: "The time the user was deleted.",
				Type:        proto.ColumnType_TIMESTAMP,
				Transform:   transform.FromField("DeletionTime").NullIfZero(),
			},
			{
				Name:        "org_unit_path",
				Description: "The full path to the user's organizational unit.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("OrgUnitPath"),
			},
			{
				Name:        "customer_id",
				Description: "The customer ID.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("CustomerId"),
			},
			{
				Name:        "etag",
				Description: "The ETag of the resource.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "hash_function",
				Description: "The hash function used for the password.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("HashFunction"),
			},
			{
				Name:        "password",
				Description: "The user's password (write-only).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "recovery_email",
				Description: "The user's recovery email address.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("RecoveryEmail"),
			},
			{
				Name:        "recovery_phone",
				Description: "The user's recovery phone number.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("RecoveryPhone"),
			},
			{
				Name:        "thumbnail_photo_etag",
				Description: "The ETag of the user's thumbnail photo.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ThumbnailPhotoEtag"),
			},
			{
				Name:        "thumbnail_photo_url",
				Description: "The URL of the user's thumbnail photo.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ThumbnailPhotoUrl"),
			},
			{
				Name:        "addresses",
				Description: "The user's address details.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "aliases",
				Description: "The user's email aliases.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "emails",
				Description: "The user's email addresses.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "external_ids",
				Description: "The user's external IDs.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("ExternalIds"),
			},
			{
				Name:        "gender",
				Description: "The user's gender information.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "ims",
				Description: "The user's instant messaging details.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "keywords",
				Description: "The user's keywords.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "languages",
				Description: "The user's language preferences.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "locations",
				Description: "The user's location information.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "notes",
				Description: "Notes about the user.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "organizations",
				Description: "The user's organization information.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "phones",
				Description: "The user's phone numbers.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "posix_accounts",
				Description: "The user's POSIX account information.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("PosixAccounts"),
			},
			{
				Name:        "relations",
				Description: "The user's relations.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "ssh_public_keys",
				Description: "The user's SSH public keys.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("SshPublicKeys"),
			},
			{
				Name:        "websites",
				Description: "The user's websites.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "custom_schemas",
				Description: "Custom fields for the user.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("CustomSchemas"),
			},
		},
	}
}

//// LIST FUNCTION

func listDirectoryUsers(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	service, err := AdminService(ctx, d)
	if err != nil {
		return nil, err
	}

	fields := googleapi.Field("users(id,primaryEmail,name,isAdmin,isDelegatedAdmin,suspended,suspensionReason,archived,agreedToTerms,changePasswordAtNextLogin,includeInGlobalAddressList,ipWhitelisted,isMailboxSetup,lastLoginTime,creationTime,deletionTime,orgUnitPath,customerId,etag,hashFunction,password,recoveryEmail,recoveryPhone,thumbnailPhotoEtag,thumbnailPhotoUrl,addresses,aliases,emails,externalIds,gender,ims,keywords,languages,locations,notes,organizations,phones,posixAccounts,relations,sshPublicKeys,websites,customSchemas)")

	maxResults := int64(100)
	if d.QueryContext.Limit != nil {
		if *d.QueryContext.Limit < maxResults {
			maxResults = *d.QueryContext.Limit
		}
	}

	resp := service.Users.List().Customer("my_customer").Fields(fields).MaxResults(maxResults)

	if d.EqualsQualString("primary_email") != "" {
		resp = resp.Query("email:" + d.EqualsQualString("primary_email"))
	}

	if d.EqualsQualString("org_unit_path") != "" {
		resp = resp.Query("orgUnitPath:" + d.EqualsQualString("org_unit_path"))
	}

	err = resp.Pages(ctx, func(page *admin.Users) error {
		for _, user := range page.Users {
			d.StreamListItem(ctx, user)

			if d.RowsRemaining(ctx) == 0 {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return nil, nil
}
