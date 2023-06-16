.PHONY:  as-hegemony-detector atlas-delay-detector build clean help

MAKEFLAGS += --no-print-directory

as-hegemony-detector:
	docker run --rm --name as-hegemony-detector internethealthreport/anomalydetector hegemonydetector.conf

atlas-delay-detector:
	docker run --rm --name atlas-delay-detector internethealthreport/anomalydetector atlasdelaydetector.conf

build:
	docker build --tag internethealthreport/anomalydetector .

clean:
	docker

help: ## Show this help.
	@echo "Make Anomaly Detector Docker image & Container"

# help:       	## Show this help.
# @echo "Make Application Docker Images and Containers using Docker-Compose files in 'docker' Dir."
# @awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m (default: help)\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

