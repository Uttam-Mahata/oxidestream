package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

var (
	JobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "oxidestream_jobs_total",
			Help: "Total number of jobs",
		},
		[]string{"status"},
	)

	TasksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "oxidestream_tasks_total",
			Help: "Total number of tasks",
		},
		[]string{"stage", "status"},
	)

	WorkersActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "oxidestream_workers_active",
			Help: "Number of active workers",
		},
	)

	QueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "oxidestream_queue_depth",
			Help: "Current queue depth",
		},
	)

	JobDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "oxidestream_job_duration_seconds",
			Help:    "Job execution duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	TaskDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "oxidestream_task_duration_seconds",
			Help:    "Task execution duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	CPUUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "oxidestream_cpu_usage",
			Help: "CPU usage per worker",
		},
		[]string{"worker_id"},
	)

	MemoryUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "oxidestream_memory_usage_bytes",
			Help: "Memory usage per worker in bytes",
		},
		[]string{"worker_id"},
	)
)

func InitMetrics() {
	prometheus.MustRegister(
		JobsTotal,
		TasksTotal,
		WorkersActive,
		QueueDepth,
		JobDuration,
		TaskDuration,
		CPUUsage,
		MemoryUsage,
	)
}

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
