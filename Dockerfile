# Build stage - use Amazon Linux 2023 with Rust
FROM amazonlinux:2023 AS builder

# Install build dependencies
RUN dnf install -y gcc gcc-c++ make openssl-devel pkg-config && \
    dnf clean all

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Create app directory
WORKDIR /app

# Copy manifests first for better caching
COPY Cargo.toml ./

# Copy source code
COPY src/ ./src/
COPY connectors/ ./connectors/
COPY tests/ ./tests/

# Build release binary
RUN cargo build --release

# Runtime stage - Amazon Linux 2023
FROM amazonlinux:2023

# Install shadow-utils for useradd, clean up to reduce image size
RUN dnf install -y shadow-utils && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Create non-root user for security
RUN useradd -r -s /sbin/nologin solidafy

# Copy binary from builder
COPY --from=builder /app/target/release/solidafy-cdk /usr/local/bin/solidafy-cdk

# Set ownership
RUN chown solidafy:solidafy /usr/local/bin/solidafy-cdk

# Switch to non-root user
USER solidafy

# Default port (can override with -e PORT=3000)
ENV PORT=8080
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:${PORT}/health || exit 1

# Default: run HTTP server on $PORT
# Override with: docker run --rm solidafy-cdk list
ENTRYPOINT ["solidafy-cdk"]
CMD ["serve", "--port", "8080"]
