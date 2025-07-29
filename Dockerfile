FROM openjdk:17-jre-slim

# Install necessary packages
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Create application user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /opt/app

# Copy application JAR
COPY target/transaction-processor-*.jar app.jar

# Copy configuration files
COPY src/main/resources/profiles/ /opt/app/config/profiles/

# Create logs directory
RUN mkdir -p /var/log/transaction-processor && \
    chown -R appuser:appuser /opt/app /var/log/transaction-processor

# Switch to application user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

# Expose port
EXPOSE 8080

# JVM optimization for containerized environment
ENV JAVA_OPTS="-XX:+UseContainerSupport \
               -XX:MaxRAMPercentage=75.0 \
               -XX:+UseG1GC \
               -XX:+UseStringDeduplication \
               -XX:+OptimizeStringConcat \
               -Djava.security.egd=file:/dev/./urandom"

# Application configuration
ENV SPRING_PROFILES_ACTIVE=production
ENV PROCESSOR_PROFILE=production
ENV PROCESSOR_PROFILE_DIR=/opt/app/config/profiles
ENV LOG_FILE_PATH=/var/log/transaction-processor/application.log

# Start application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]