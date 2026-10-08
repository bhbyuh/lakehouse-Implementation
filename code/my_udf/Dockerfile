# Stage 1: build the jar with Maven
FROM maven:3.9-amazoncorretto-17-al2023 AS build
WORKDIR /app

# Copy the pom first so dependency downloads are cached between builds
COPY pom.xml .
RUN mvn -q dependency:go-offline

# Copy the source and build
COPY src ./src
RUN mvn -q clean package

# Stage 2: contains only the jar, so `--output` writes just that file to your machine
FROM scratch AS export
COPY --from=build /app/target/my-udfs.jar /


# Command To run this docker file
# docker build --output type=local,dest=./target .