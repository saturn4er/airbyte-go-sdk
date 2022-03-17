package apisource

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/saturn4er/airbyte-go-sdk"
)

type APISource struct {
	baseURL string
}

type LastSyncTime struct {
	Timestamp int64 `json:"timestamp"`
}

type HTTPConfig struct {
	// DummyJSON doesn't require authentication
	Limit int `json:"limit"` // Optional: limit number of records (default: 30)
}

func NewAPISource(baseURL string) airbyte.Source {
	return APISource{
		baseURL: baseURL,
	}
}

func (h APISource) Spec(logTracker airbyte.LogTracker) (*airbyte.ConnectorSpecification, error) {
	if err := logTracker.Log(airbyte.LogLevelInfo, "Running Spec"); err != nil {
		return nil, err
	}
	return &airbyte.ConnectorSpecification{
		DocumentationURL:      "https://dummyjson.com",
		ChangeLogURL:          "https://github.com/bitstrapped/airbyte-go-sdk",
		SupportsIncremental:   false,
		SupportsNormalization: true,
		SupportsDBT:           true,
		SupportedDestinationSyncModes: []airbyte.DestinationSyncMode{
			airbyte.DestinationSyncModeOverwrite,
		},
		ProtocolVersion: "0.5.2", // Airbyte Protocol v0.5.2
		ConnectionSpecification: airbyte.ConnectionSpecification{
			Title:       "DummyJSON Source",
			Description: "Example source connector that fetches data from dummyjson.com API",
			Type:        "object",
			Required:    []airbyte.PropertyName{},
			Properties: airbyte.Properties{
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"limit": {
						Description: "Maximum number of records to fetch per stream (default: 30)",
						Examples:    []string{"10", "30", "100"},
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{
								airbyte.Integer,
							},
						},
					},
				},
			},
		},
	}, nil
}

func (h APISource) Check(srcCfgPath string, logTracker airbyte.LogTracker) error {
	if err := logTracker.Log(airbyte.LogLevelDebug, "validating api connection"); err != nil {
		return err
	}
	var srcCfg HTTPConfig
	err := airbyte.UnmarshalFromPath(srcCfgPath, &srcCfg)
	if err != nil {
		return err
	}

	// Test connection by fetching a single user
	resp, err := http.Get(fmt.Sprintf("%s/users/1", h.baseURL))
	if err != nil {
		return fmt.Errorf("failed to connect to DummyJSON API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code from DummyJSON API: %d", resp.StatusCode)
	}

	return nil
}

func (h APISource) Discover(srcCfgPath string, logTracker airbyte.LogTracker) (*airbyte.Catalog, error) {
	var srcCfg HTTPConfig
	err := airbyte.UnmarshalFromPath(srcCfgPath, &srcCfg)
	if err != nil {
		return nil, err
	}

	return &airbyte.Catalog{Streams: []airbyte.Stream{
		{
			Name: "users",
			JSONSchema: airbyte.Properties{
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"id": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer},
						},
						Description: "User ID",
					},
					"firstName": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Description: "First name",
					},
					"lastName": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Description: "Last name",
					},
					"email": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Description: "Email address",
					},
					"phone": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Description: "Phone number",
					},
					"age": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer},
						},
						Description: "Age",
					},
				},
			},
			SupportedSyncModes: []airbyte.SyncMode{
				airbyte.SyncModeFullRefresh,
			},
			SourceDefinedCursor: false,
			Namespace:           "dummyjson",
		},
		{
			Name: "products",
			JSONSchema: airbyte.Properties{
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"id": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer},
						},
						Description: "Product ID",
					},
					"title": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Description: "Product title",
					},
					"description": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Description: "Product description",
					},
					"price": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Number},
						},
						Description: "Product price",
					},
					"category": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Description: "Product category",
					},
					"brand": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Description: "Product brand",
					},
				},
			},
			SupportedSyncModes: []airbyte.SyncMode{
				airbyte.SyncModeFullRefresh,
			},
			SourceDefinedCursor: false,
			Namespace:           "dummyjson",
		},
	}}, nil
}

type User struct {
	ID        int    `json:"id"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
	Age       int    `json:"age"`
}

type Product struct {
	ID          int     `json:"id"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Price       float64 `json:"price"`
	Category    string  `json:"category"`
	Brand       string  `json:"brand"`
}

type UsersResponse struct {
	Users []User `json:"users"`
	Total int    `json:"total"`
	Skip  int    `json:"skip"`
	Limit int    `json:"limit"`
}

type ProductsResponse struct {
	Products []Product `json:"products"`
	Total    int       `json:"total"`
	Skip     int       `json:"skip"`
	Limit    int       `json:"limit"`
}

func (h APISource) Read(sourceCfgPath string, prevStatePath string, configuredCat *airbyte.ConfiguredCatalog,
	tracker airbyte.MessageTracker) error {
	if err := tracker.Log(airbyte.LogLevelInfo, "Running read"); err != nil {
		return err
	}
	var src HTTPConfig
	err := airbyte.UnmarshalFromPath(sourceCfgPath, &src)
	if err != nil {
		return err
	}

	// Set default limit if not specified
	limit := src.Limit
	if limit == 0 {
		limit = 30
	}

	// see if there is a last sync
	var st LastSyncTime
	_ = airbyte.UnmarshalFromPath(sourceCfgPath, &st)
	if st.Timestamp <= 0 {
		st.Timestamp = -1
	}

	for _, stream := range configuredCat.Streams {
		if stream.Stream.Name == "users" {
			// Emit estimate for progress tracking (protocol v0.5.2 feature)
			rowEstimate := int64(limit)
			if err := airbyte.EmitEstimate(tracker.Trace, stream.Stream.Name, &stream.Stream.Namespace,
				airbyte.EstimateTypeStream, &rowEstimate, nil); err != nil {
				tracker.Log(airbyte.LogLevelWarn, fmt.Sprintf("Failed to emit estimate: %v", err))
			}

			var usersResp UsersResponse
			uri := fmt.Sprintf("%s/users?limit=%d", h.baseURL, limit)
			if err := httpGet(uri, &usersResp); err != nil {
				return fmt.Errorf("failed to fetch users: %w", err)
			}

			for _, user := range usersResp.Users {
				err := tracker.Record(user, stream.Stream.Name, stream.Stream.Namespace)
				if err != nil {
					return err
				}
			}
		}

		if stream.Stream.Name == "products" {
			// Emit estimate for progress tracking (protocol v0.5.2 feature)
			rowEstimate := int64(limit)
			if err := airbyte.EmitEstimate(tracker.Trace, stream.Stream.Name, &stream.Stream.Namespace,
				airbyte.EstimateTypeStream, &rowEstimate, nil); err != nil {
				tracker.Log(airbyte.LogLevelWarn, fmt.Sprintf("Failed to emit estimate: %v", err))
			}

			var productsResp ProductsResponse
			uri := fmt.Sprintf("%s/products?limit=%d", h.baseURL, limit)
			if err := httpGet(uri, &productsResp); err != nil {
				return fmt.Errorf("failed to fetch products: %w", err)
			}

			for _, product := range productsResp.Products {
				err := tracker.Record(product, stream.Stream.Name, stream.Stream.Namespace)
				if err != nil {
					return err
				}
			}
		}
	}

	return tracker.State(airbyte.StateTypeLegacy, &LastSyncTime{
		Timestamp: time.Now().UnixMilli(),
	})
}

func httpGet(uri string, v interface{}) error {
	resp, err := http.Get(uri)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}
