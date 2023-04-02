package bus_golang_publisher_aws_kinesis

import (
	"context"
	"fmt"

	"github.com/al-kimmel-serj/bus-golang"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/proto"
)

type Publisher[Payload proto.Message] struct {
	kinesisClient    *kinesis.Client
	kinesisStreamARN string
}

func New[Payload proto.Message](
	kinesisClient *kinesis.Client,
	kinesisStreamARN string,
) (*Publisher[Payload], error) {
	return &Publisher[Payload]{
		kinesisClient:    kinesisClient,
		kinesisStreamARN: kinesisStreamARN,
	}, nil
}

func (p *Publisher[Payload]) Publish(ctx context.Context, events []bus.Event[Payload]) error {
	if len(events) == 0 {
		return nil
	}

	records := make([]types.PutRecordsRequestEntry, 0, len(events))
	for _, event := range events {
		msg, err := proto.Marshal(event.EventPayload)
		if err != nil {
			return fmt.Errorf("proto.Marshal error: %w", err)
		}

		records = append(records, types.PutRecordsRequestEntry{
			Data:         msg,
			PartitionKey: (*string)(&event.EventKey),
		})
	}

	_, err := p.kinesisClient.PutRecords(ctx, &kinesis.PutRecordsInput{
		Records:   records,
		StreamARN: &p.kinesisStreamARN,
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *Publisher[Payload]) Stop() error {
	return nil
}
