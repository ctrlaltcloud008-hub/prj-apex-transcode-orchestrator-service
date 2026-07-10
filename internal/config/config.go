package config

import (
	"strings"

	"github.com/ctrlaltcloud008-hub/prj-apex-core-modules/pkg/config"
	"github.com/spf13/viper"
)

type OrchestratorConfig struct {
	appEnv                 string
	port                   string
	service                string
	region                 string
	projectID              string
	spannerDB              string
	subscription           string
	completionSubscription string
	normalTopic            string
	priorityTopic          string
	maxRenditionAttempts   int64
	stallThresholdMinutes  int64
	outputGCSBucket        string
}

func LoadOrchestratorConfig() (*OrchestratorConfig, error) {
	v := viper.New()
	v.SetDefault("PORT", "8080")
	v.SetDefault("APP_ENV", "local")
	v.SetDefault("SERVICE", "transcode-orchestrator")
	v.SetDefault("REGION", "asia-south1")
	v.SetDefault("PROJECT_ID", "apex-494315")
	v.SetDefault("SPANNER_DATABASE", "projects/test-project/instances/test-instance/databases/test-database")
	v.SetDefault("SUBSCRIPTION", "projects/test-project/subscriptions/sub-transcode-orch")
	v.SetDefault("COMPLETION_SUBSCRIPTION", "projects/test-project/subscriptions/sub-transcode-completion")
	v.SetDefault("NORMAL_TOPIC", "projects/test-project/topics/transcode.job.requested")
	v.SetDefault("PRIORITY_TOPIC", "projects/test-project/topics/transcode.job.requested.priority")
	v.SetDefault("MAX_RENDITION_ATTEMPTS", 3)
	v.SetDefault("STALL_THRESHOLD_MINUTES", 15)
	v.SetDefault("OUTPUT_GCS_BUCKET", "prj-apex-trancoded-data")

	if err := config.LoadConfig(v, "config"); err != nil {
		return nil, err
	}

	cfg := &OrchestratorConfig{
		appEnv:                 v.GetString("APP_ENV"),
		port:                   normalizePort(v.GetString("PORT")),
		service:                v.GetString("SERVICE"),
		region:                 v.GetString("REGION"),
		projectID:              v.GetString("PROJECT_ID"),
		spannerDB:              v.GetString("SPANNER_DATABASE"),
		subscription:           v.GetString("SUBSCRIPTION"),
		completionSubscription: v.GetString("COMPLETION_SUBSCRIPTION"),
		normalTopic:            v.GetString("NORMAL_TOPIC"),
		priorityTopic:          v.GetString("PRIORITY_TOPIC"),
		maxRenditionAttempts:   v.GetInt64("MAX_RENDITION_ATTEMPTS"),
		stallThresholdMinutes:  v.GetInt64("STALL_THRESHOLD_MINUTES"),
		outputGCSBucket:        v.GetString("OUTPUT_GCS_BUCKET"),
	}

	return cfg, nil
}

func normalizePort(port string) string {
	port = strings.TrimSpace(port)
	if port == "" {
		return ":8080"
	}
	if strings.HasPrefix(port, ":") {
		return port
	}
	return ":" + port
}

func (c *OrchestratorConfig) AppEnv() string                 { return c.appEnv }
func (c *OrchestratorConfig) Port() string                   { return c.port }
func (c *OrchestratorConfig) Service() string                { return c.service }
func (c *OrchestratorConfig) Region() string                 { return c.region }
func (c *OrchestratorConfig) ProjectID() string              { return c.projectID }
func (c *OrchestratorConfig) SpannerDatabase() string        { return c.spannerDB }
func (c *OrchestratorConfig) Subscription() string           { return c.subscription }
func (c *OrchestratorConfig) CompletionSubscription() string { return c.completionSubscription }
func (c *OrchestratorConfig) NormalTopic() string            { return c.normalTopic }
func (c *OrchestratorConfig) PriorityTopic() string          { return c.priorityTopic }
func (c *OrchestratorConfig) MaxRenditionAttempts() int64    { return c.maxRenditionAttempts }
func (c *OrchestratorConfig) StallThresholdMinutes() int64   { return c.stallThresholdMinutes }
func (c *OrchestratorConfig) OutputGCSBucket() string        { return c.outputGCSBucket }
