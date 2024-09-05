FROM postgis/postgis:latest

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=DnextCropTest
ENV POSTGRES_DB=geospatialdb

EXPOSE 5432