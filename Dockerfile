# Build stage
FROM maven:3.8.6-openjdk-11 AS build
WORKDIR /app
COPY . .
RUN mvn clean package -DskipTests

# Run stage
FROM openjdk:11-jre-slim
WORKDIR /app
# Sao chép file JAR vào container
COPY target/producer-app-1.0-SNAPSHOT.jar /app/producer-app-1.0-SNAPSHOT.jar

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/actuator/health || exit 1

# Expose port
EXPOSE 8080

# Run the application
ENTRYPOINT ["java", "-jar", "producer-app-1.0-SNAPSHOT.jar"]
