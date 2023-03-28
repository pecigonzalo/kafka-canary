package services_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pecigonzalo/kafka-canary/internal/canary"
	"github.com/pecigonzalo/kafka-canary/internal/client"
	"github.com/pecigonzalo/kafka-canary/internal/services"
	"github.com/pecigonzalo/kafka-canary/internal/services/mocks"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
)

func TestServices(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Services Suite")
}

var _ = Describe("Internal/Services", func() {
	var admin *mocks.Admin
	var topicService services.TopicService

	BeforeEach(func() {
		admin = mocks.NewAdmin(GinkgoT())
		topicService = services.NewTopicService(
			admin, canary.Config{}, &zerolog.Logger{},
		)
	})

	AfterEach(func() {
		prometheus.DefaultRegisterer = prometheus.NewRegistry()
	})

	Context("When topic does not exist", func() {
		BeforeEach(func() {
			admin.On("GetBrokers", mock.Anything).
				Return([]client.BrokerInfo{
					{ID: 1, Rack: "rack1"},
					{ID: 2, Rack: "rack2"},
					{ID: 3, Rack: "rack3"},
				}, nil).
				Maybe()

			admin.On("GetTopic", mock.Anything, mock.Anything).
				Return(client.TopicInfo{}, kafka.UnknownTopicOrPartition).
				Maybe()
		})
		It("should create the topic", func() {
			admin.EXPECT().CreateTopic(
				mock.Anything,
				mock.Anything,
				mock.Anything,
				mock.Anything,
			).Return(nil).Once()

			_, err := topicService.Reconcile(context.Background())

			Expect(err).NotTo(HaveOccurred())
		})
	})
	Context("When topic exist", func() {
		BeforeEach(func() {
			admin.On("GetBrokers", mock.Anything).
				Return([]client.BrokerInfo{
					{ID: 1, Rack: "rack1"},
					{ID: 2, Rack: "rack2"},
					{ID: 3, Rack: "rack3"},
				}, nil).
				Maybe()
		})

		Context("and partitions match brokers", func() {
			BeforeEach(func() {
				admin.EXPECT().GetTopic(mock.Anything, mock.Anything).
					Return(client.TopicInfo{
						Name: "fake-topic",
						Partitions: []client.PartitionAssignment{
							{
								ID:       0,
								Leader:   1,
								Replicas: []int{1, 2, 3},
							},
							{
								ID:       1,
								Leader:   1,
								Replicas: []int{1, 2, 3},
							},
							{
								ID:       2,
								Leader:   1,
								Replicas: []int{1, 2, 3},
							},
						},
					}, nil).Maybe()
			})

			It("reconcile the configuration", func() {
				admin.EXPECT().UpdateTopicConfig(mock.Anything, mock.Anything, mock.Anything).
					Return(nil)

				_, err := topicService.Reconcile(context.Background())
				Expect(err).Should(BeNil())
			})
		})
		Context("and partitions do not match brokers", func() {
			BeforeEach(func() {
				admin.EXPECT().GetTopic(mock.Anything, mock.Anything).
					Return(client.TopicInfo{
						Name: "fake-topic",
						Partitions: []client.PartitionAssignment{
							{
								ID:       0,
								Leader:   1,
								Replicas: []int{1, 2, 3},
							},
						},
					}, nil).Once().Maybe()

				admin.EXPECT().GetTopic(mock.Anything, mock.Anything).
					Return(client.TopicInfo{
						Name: "fake-topic",
						Partitions: []client.PartitionAssignment{
							{
								ID:       0,
								Leader:   1,
								Replicas: []int{1, 2, 3},
							},
							{
								ID:       1,
								Leader:   1,
								Replicas: []int{1, 2, 3},
							},
							{
								ID:       2,
								Leader:   1,
								Replicas: []int{1, 2, 3},
							},
						},
					}, nil).Maybe()

				// To assert
				admin.EXPECT().UpdateTopicConfig(mock.Anything, mock.Anything, mock.Anything).
					Return(nil).Maybe()
				admin.EXPECT().AssignPartitions(mock.Anything, mock.Anything, mock.Anything).
					Return(nil).Maybe()
				admin.EXPECT().AddPartitions(mock.Anything, mock.Anything, mock.Anything).
					Return(nil).Maybe()
				admin.EXPECT().RunLeaderElection(mock.Anything, mock.Anything, mock.Anything).
					Return(nil).Maybe()
			})
			It("Reconcile the topic", func() {
				got, err := topicService.Reconcile(context.Background())
				Expect(err).Should(BeNil())
				Expect(got).Should(Equal(services.TopicReconcileResult{
					Assignments: []int{0, 1, 2},
					Leaders: map[int]int{
						0: 1,
						1: 1,
						2: 1,
					},
					RefreshProducerMetadata: true,
				}))

				By("Reconciling the config")
				admin.AssertNumberOfCalls(GinkgoT(), "UpdateTopicConfig", 1)

				By("Reconciling the partitions")
				admin.AssertNumberOfCalls(GinkgoT(), "AssignPartitions", 1)
				admin.AssertNumberOfCalls(GinkgoT(), "AddPartitions", 1)

				By("Running a leader election")
				admin.AssertNumberOfCalls(GinkgoT(), "RunLeaderElection", 1)
			})
		})
	})
})
