"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
station_topic = app.topic("connect-stations", value_type=Station)
# TODO: Define the output Kafka Topic
transformed_station_topic = app.topic("transformed-stations", value_type=TransformedStation, partitions=1)
# TODO: Define a Faust Table
#table = app.Table(
#    # "TODO",
#    # default=TODO,
#    partitions=1,
#    changelog_topic=out_topic,
#)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#

def transform(station):
    line = None

    if (station.red is True):
        line = 'red'
    if (station.blue is True):
        line = 'blue'
    if (station.green is True):
        line = 'green'

    return TransformedStation(
        station_id = station.station_id,
        station_name = station.station_name,
        order = station.order,
        line = line
    )


@app.agent(station_topic)
async def transform_station(stream):
    async for key, station in stream.items():
        if (station.green is True or station.blue is True or station.red is True):
            #TODO: what is the key?
            await transformed_station_topic.send(key=station.station_id, value=transform(station))


if __name__ == "__main__":
    app.main()
