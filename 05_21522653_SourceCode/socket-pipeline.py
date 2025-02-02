import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import socket
import json

class ReadFromSocket(beam.PTransform):
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port

    def expand(self, pcoll):
        return pcoll | beam.Impulse() | beam.FlatMap(self._read_socket)

    def _read_socket(self, _):
        # This function will be called to read from the socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            buffer = ""
            while True:
                data = s.recv(1024)
                if not data:
                    break
                buffer += data.decode('utf-8')
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    yield line.strip()

class ParseAndAddTimestamp(beam.DoFn):
    def process(self, element, timestamp=beam.DoFn.TimestampParam):
        # Parse incoming data and add timestamp
        try:
            plant = json.loads(element)
            # Ensure 'season' is a Unix timestamp in seconds
            event_timestamp = float(plant['season'])
            yield beam.window.TimestampedValue(plant, event_timestamp)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"Invalid data: {element} | Error: {e}")

class PrintResult(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        print(f"Window {window_start} - {window_end}: {element}")
        yield element

def withtimestamps_streaming_pipeline():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True  # Enable streaming mode

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | 'Read from socket' >> ReadFromSocket('localhost', 9990)
            | 'Parse JSON and Add Timestamps' >> beam.ParDo(ParseAndAddTimestamp())
            | 'Window' >> beam.WindowInto(
                beam.window.FixedWindows(5),
                trigger=beam.trigger.AfterWatermark(),
                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING
            )
            | 'Extract name' >> beam.Map(lambda plant: plant['name'])
            | 'PairWithOne' >> beam.Map(lambda name: (name, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Add dummy key' >> beam.Map(lambda x: ('dummy', x))
            | 'Group by key' >> beam.GroupByKey()
            | 'Convert to dict' >> beam.Map(lambda kv: dict(kv[1]))
            | 'Format result' >> beam.Map(
                lambda d: ', '.join([f'{k}: {v}' for k, v in d.items()])
            )
            | 'Print results' >> beam.ParDo(PrintResult())
        )

if __name__ == "__main__":
    withtimestamps_streaming_pipeline()




