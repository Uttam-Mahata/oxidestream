package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	masterURL := "http://localhost:8080/submit"

	payload := map[string]interface{}{
		"map_sql":        "SELECT r.user_id, c.category_name, COUNT(1) as rating_count, SUM(r.rating) as rating_sum FROM input r JOIN category_lookup c ON r.category = c.category GROUP BY r.user_id, c.category_name",
		"reduce_sql":     "SELECT user_id, category_name, SUM(rating_count) as total_ratings, SUM(rating_sum) as total_rating_sum FROM input GROUP BY user_id, category_name",
		"input_files": []string{
			"/home/uttam/oxidestream/tests/data/part-0.csv",
			"/home/uttam/oxidestream/tests/data/part-1.csv",
			"/home/uttam/oxidestream/tests/data/part-2.csv",
			"/home/uttam/oxidestream/tests/data/category_lookup.csv",
		},
		"num_partitions": 4,
		"output_dir":     "/home/uttam/oxidestream/tests/output",
	}

	reqBytes, err := json.Marshal(payload)
	if err != nil {
		log.Fatalf("Failed to marshal payload: %v", err)
	}

	fmt.Printf("Submitting job to Master REST API: %s\n", masterURL)
	resp, err := http.Post(masterURL, "application/json", bytes.NewBuffer(reqBytes))
	if err != nil {
		log.Fatalf("Failed to post job: %v", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Failed to read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("Job submission failed (HTTP %d): %s", resp.StatusCode, string(respBytes))
	}

	var submitResp map[string]string
	if err := json.Unmarshal(respBytes, &submitResp); err != nil {
		log.Fatalf("Failed to parse response JSON: %v", err)
	}

	jobID := submitResp["job_id"]
	fmt.Printf("Job submitted successfully! Job ID: %s\n", jobID)

	statusURL := fmt.Sprintf("http://localhost:8080/status?job_id=%s", jobID)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		sResp, err := http.Get(statusURL)
		if err != nil {
			log.Printf("Error checking job status: %v", err)
			continue
		}

		sBytes, err := io.ReadAll(sResp.Body)
		sResp.Body.Close()
		if err != nil {
			log.Printf("Failed to read status response body: %v", err)
			continue
		}

		var statusResp map[string]string
		if err := json.Unmarshal(sBytes, &statusResp); err != nil {
			log.Printf("Failed to parse status JSON: %v", err)
			continue
		}

		status := statusResp["status"]
		fmt.Printf("[%s] Job Status: %s\n", time.Now().Format("15:04:05.000"), status)

		if status == "COMPLETED" {
			fmt.Println("Integration test job executed and completed successfully!")
			os.Exit(0)
		} else if status == "FAILED" {
			fmt.Println("Integration test job failed!")
			os.Exit(1)
		}
	}
}
