package awsssmprovider

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/internal"
	"strings"
)

type provider struct {
	ssmClient *ssm.Client
}

const schemeName = "ssm"

func New(cfg aws.Config) confmap.Provider {
	return &provider{ssmClient: ssm.NewFromConfig(cfg)}
}

func (emp *provider) Retrieve(ctx context.Context, uri string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
	if !strings.HasPrefix(uri, schemeName+":") {
		return nil, fmt.Errorf("%q uri is not supported by %q provider", uri, schemeName)
	}

	resp, err := emp.ssmClient.GetParameter(ctx, &ssm.GetParameterInput{
		Name: aws.String(uri[len(schemeName)+1:]),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to download the config from aws ssm %q, with err: %w ", uri, err)
	}
	return internal.NewRetrievedFromYAML([]byte(*resp.Parameter.Value))
}

func (*provider) Scheme() string {
	return schemeName
}

func (*provider) Shutdown(context.Context) error {
	return nil
}
