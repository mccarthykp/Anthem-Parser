/* -~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
CPU: 2.3GHz Quad Core Intel Core i7
RAM: 32GB

Execution Time: 6m 51s
-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~ */
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

// Define structs to match the JSON structure
type ReportingStructure struct {
	InNetworkFiles []InNetworkFile `json:"in_network_files"`
	ReportingPlans []ReportingPlan `json:"reporting_plans"`
}

type ReportingPlan struct {
	PlanNames string `json:"plan_name"`
}

type InNetworkFile struct {
	Location string `json:"location"`
}

func main() {
	startTime := time.Now()

	indexFileURL := "https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/anthem/2024-08-01_anthem_index.json.gz"

	// Create JSON decoder
	decoder, err := createJSONDecoder(indexFileURL)
	if err != nil {
		log.Fatalf("failed to create JSON decoder: %v", err)
	}

	// Create output file
	outFile, err := os.Create("Anthem_NY_Filter.txt")
	if err != nil {
		log.Fatalf("failed to create output file: %v", err)
	}
	defer outFile.Close()

	bufferedWriter := bufio.NewWriter(outFile)
	defer bufferedWriter.Flush()

	seen := make(map[string]bool)

	// Decode top-level JSON object
	_, err = decoder.Token()
	if err != nil {
		log.Fatalf("failed to read start of JSON object: %v", err)
	}

	// Iterate over top-level fields
	for decoder.More() {
		// Read field name
		fieldName, err := decoder.Token()
		if err != nil {
			log.Fatalf("failed to read field name: %v", err)
		}

		// Process the "reporting_structure" field
		if fieldName == "reporting_structure" {
			_, err = decoder.Token()
			if err != nil {
				log.Fatalf("failed to read start of reporting_structure array: %v", err)
			}

			// Process each ReportingStructure in the array
			for decoder.More() {
				var reportingStructure ReportingStructure

				// Decode the current ReportingStructure object
				err := decoder.Decode(&reportingStructure)
				if err != nil {
					log.Fatalf("failed to decode ReportingStructure: %v", err)
				}

				// Check if any ReportingPlan has both "PPO" && "NY" || "New York" in plan_name
				hasRelevantPlan := false
				for _, plan := range reportingStructure.ReportingPlans {
					if isPlanRelevant(plan.PlanNames) {
						hasRelevantPlan = true
						break
					}

					if !hasRelevantPlan {
						continue // Skip this ReportingStructure if no relevant plans
					}
				}

				// Process each InNetworkFile
				for _, file := range reportingStructure.InNetworkFiles {
					// Check if the Location contains "anthem/NY"
					if !containsLocationFilter(file.Location) {
						continue // Skip this entry
					}

					// Create a key for the map using location
					key := fmt.Sprintf(file.Location)

					// Check if the entry has already been seen
					if _, exists := seen[key]; exists {
						continue // Skip this entry
					}
					// Mark the entry as seen
					seen[key] = true

					// Write the URL to the output file
					_, err = bufferedWriter.WriteString(file.Location + "\n")
					if err != nil {
						log.Fatalf("failed to write to output file: %v", err)
					}

					// Flush periodically if buffer is filling up
					if bufferedWriter.Available() <= 2048 {
						bufferedWriter.Flush()
					}
				}
			}
		}
	}

	// Flush the buffered writer to ensure all data is written
	err = bufferedWriter.Flush()
	if err != nil {
		log.Fatalf("failed to flush buffered writer: %v", err)
	}

	duration := time.Since(startTime)
	fmt.Printf("Output file created successfully. Execution time: %v\n", duration)
}

/* -~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
createJSONDecoder creates and returns a JSON decoder from a given URL */
func createJSONDecoder(url string) (*json.Decoder, error) {
	// Create an HTTP request to stream the file
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %v", err)
	}

	// Check for successful response
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch file: %v", resp.Status)
	}

	// Create pipe for streaming
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		_, err := io.Copy(pw, resp.Body)
		if err != nil {
			log.Printf("failed to copy response body to pipe: %v", err)
		}
	}()

	// Create Gzip reader
	gz, err := gzip.NewReader(pr)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %v", err)
	}

	// Return JSON decoder
	return json.NewDecoder(gz), nil
}

/* -~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
isPlanRelevant checks if a planName contains "PPO" && "NY" || "New York" */
func isPlanRelevant(planName string) bool {
	planNameLower := strings.ToLower(planName)
	return (strings.Contains(planNameLower, "ny") || strings.Contains(planNameLower, "new york")) && (strings.Contains(planNameLower, "ppo"))
}

/* -~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~-~
containsLocationFilter checks if a location contains "anthem/NY" */
func containsLocationFilter(location string) bool {
	return strings.Contains(location, "anthem/NY")
}
