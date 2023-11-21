package main

import (
	"flag"
	"log"
	"os"
	"path"
	"strings"

	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

var help = flag.Bool("help", false, "Converts traces from OTLP JSON format to OTLP Protobuf format")
var inputFile = ""
var outputFile = ""

// This tool generates a trace dataset in the OpenTelemetry Protocol format from a fake traces generator.
func main() {
	// Define the flags.
	flag.StringVar(&inputFile, "input", outputFile, "Input OTLP JSON file")
	flag.StringVar(&outputFile, "output", outputFile, "Output OTLP Protobuf file")

	// Parse the flag
	flag.Parse()

	// Usage Demo
	if *help {
		flag.Usage()
		os.Exit(0)
	}

	inputContent, err := os.ReadFile(inputFile)
	if err != nil {
		log.Fatal("Cannot read input file: ", err)
	}
	inputSizeBytes := len(inputContent)

	jsons := strings.Split(string(inputContent), "\n")

	metricDpCount := 0

	var msg []byte
	if strings.HasPrefix(string(inputContent), "{\"resourceSpans\":") {
		combined := ptraceotlp.NewExportRequest()
		for _, json := range jsons {
			if json == "" {
				continue
			}
			request := ptraceotlp.NewExportRequest()
			err = request.UnmarshalJSON([]byte(json))
			if err != nil {
				log.Fatal("Unmarshalling error: ", err)
			}
			request.Traces().ResourceSpans().MoveAndAppendTo(combined.Traces().ResourceSpans())
		}

		// Marshal the request to bytes.
		msg, err = combined.MarshalProto()
		if err != nil {
			log.Fatal("Marshaling error: ", err)
		}
	} else if strings.HasPrefix(string(inputContent), "{\"resourceMetrics\":") {
		combined := pmetricotlp.NewExportRequest()
		for _, json := range jsons {
			if json == "" {
				continue
			}
			request := pmetricotlp.NewExportRequest()
			err = request.UnmarshalJSON([]byte(json))
			if err != nil {
				log.Fatal("Unmarshalling error: ", err)
			}
			request.Metrics().ResourceMetrics().MoveAndAppendTo(combined.Metrics().ResourceMetrics())
		}
		metricDpCount += combined.Metrics().DataPointCount()

		// Marshal the request to bytes.
		msg, err = combined.MarshalProto()
		if err != nil {
			log.Fatal("Marshaling error: ", err)
		}
	} else {
		log.Fatalf("Input file format unrecognized\n")
	}

	// Write protobuf to file
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		err = os.MkdirAll(path.Dir(outputFile), 0700)
		if err != nil {
			log.Fatal("Error creating directory: ", err)
		}
	}
	err = os.WriteFile(outputFile, msg, 0600)
	if err != nil {
		log.Fatal("Cannot write file: ", err)
	}

	log.Printf(
		"Datapoints converted %v. OTLP uncompressed bytes per datapoint %v\n",
		metricDpCount, inputSizeBytes/metricDpCount,
	)
}
