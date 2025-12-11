# ASHS (Automatic Segmentation of Hippocampal Subfields)
# Docker container definition
#
# Build:
#   docker build -t ashs:latest .
#
# Run:
#   docker run -v /path/to/data:/data ashs:latest \
#     -a /path/to/atlas -g /data/t1.nii.gz -f /data/t2.nii.gz -w /data/output
#
# Convert to Singularity:
#   singularity build ashs.sif docker://ashs:latest

FROM ubuntu:22.04

LABEL maintainer="ASHS Team"
LABEL version="2.0.0"
LABEL description="Automatic Segmentation of Hippocampal Subfields"

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    bc \
    curl \
    wget \
    ca-certificates \
    libgomp1 \
    libpng16-16 \
    libtiff5 \
    libglu1-mesa \
    libsm6 \
    libice6 \
    libxt6 \
    libxext6 \
    libxrender1 \
    parallel \
    perl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create ASHS directory structure
RUN mkdir -p /opt/ashs/bin /opt/ashs/ext/Linux

# Set environment variables
ENV ASHS_ROOT=/opt/ashs
ENV PATH=/opt/ashs/bin:/opt/ashs/ext/Linux/bin:$PATH
ENV LC_ALL=C

# Copy ASHS files
# Note: This assumes you're building from the ASHS source directory
COPY bin/ /opt/ashs/bin/
COPY ext/Linux/ /opt/ashs/ext/Linux/

# Make scripts executable
RUN chmod +x /opt/ashs/bin/*.sh \
    && chmod +x /opt/ashs/ext/Linux/bin/* 2>/dev/null || true

# Create a non-root user for running the pipeline
RUN useradd -m -s /bin/bash ashs
USER ashs
WORKDIR /home/ashs

# Default entrypoint
ENTRYPOINT ["/opt/ashs/bin/ashs_main.sh"]
CMD ["-h"]
