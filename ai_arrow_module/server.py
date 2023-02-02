"""
File contains description of ArrowFlightServer class.
"""

import os
import logging

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)

import ast
import threading

import pyarrow
import pyarrow.flight


ARROW_SERVER_PORT = int(os.environ.get("ARROW_SERVER_PORT", 8815))


class ArrowFlightServer(pyarrow.flight.FlightServerBase):
    """
    Class for launch and work as Arrow Flight Server..

    Args
    ----------
            `host` :

            `port` :

            `location` :

            `cert_chain_file_path` :

            `private_key_file_path` :

            `verify_client` :

            `root_certificates` :

            `auth_handler` :
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = ARROW_SERVER_PORT,  # Check bottom. ARROW_SERVER_PORT bottom.
        location: str = None,
        cert_chain_file_path: str = None,
        private_key_file_path: str = None,
        verify_client: bool = False,  # True - ?
        root_certificates=None,
        auth_handler=None,
        **kwargs,
    ):
        tls_certificates = []
        scheme = "grpc"  # "grpc+tcp"

        if cert_chain_file_path != None and private_key_file_path != None:
            scheme = "grpc+tls"

            with open(cert_chain_file_path, "rb") as cert_file:
                tls_cert_chain = cert_file.read()

            with open(private_key_file_path, "rb") as key_file:
                tls_private_key = key_file.read()

            tls_certificates.append((tls_cert_chain, tls_private_key))

        location = f"{scheme}://{host}:{port}"
        logging.info(f"| INIT ARROW FLIGHT SERVER: {location}")

        super(ArrowFlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client, root_certificates
        )

        self.flights = (
            {}
        )  # tables, where key/ticket_name/table_name <- descriptor <- file_name/name...

        self.host = host
        self.tls_certificates = tls_certificates

    @classmethod
    def descriptor_to_key(self, descriptor):
        return (
            descriptor.descriptor_type.value,
            descriptor.command,
            tuple(descriptor.path or tuple()),
        )

    def _make_flight_info(self, key, descriptor, table):

        if self.tls_certificates:
            location = pyarrow.flight.Location.for_grpc_tls(self.host, self.port)
        else:
            location = pyarrow.flight.Location.for_grpc_tcp(self.host, self.port)

        endpoints = [
            pyarrow.flight.FlightEndpoint(repr(key), [location]),
        ]

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return pyarrow.flight.FlightInfo(
            table.schema, descriptor, endpoints, table.num_rows, data_size
        )

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor):
        key = ArrowFlightServer.descriptor_to_key(descriptor)  # key - ticket_name
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError("Flight not found.")

    def do_put(self, context, descriptor, reader, writer):
        """Method that is called on the client side to send data to the Arrow Server"""
        key = ArrowFlightServer.descriptor_to_key(descriptor)
        logging.info(f" | KEY: {key} PUT DATA TO FLIGHT...")
        self.flights[key] = reader.read_all()
        logging.info(f" | DONE")

    def do_get(self, context, ticket):
        """Method that is called on the client side to read data from the Arrow Server"""
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            logging.info(f"| KEY: {key} NOT IN FLIGHTS !")
            return None
        logging.info(f"| KEY: {key} IN FLIGHTS !")
        logging.info(f"| SENDING DATA TO CLIENT...")
        buf = self.flights[key]
        # del self.flights[key]  # TODO: delete only if needed
        return pyarrow.flight.RecordBatchStream(buf)

    def do_remove_action(self, key, *args, **kwargs):
        descriptor = pyarrow.flight.FlightDescriptor.for_command(key)
        key = ArrowFlightServer.descriptor_to_key(descriptor)
        if key not in self.flights:
            logging.info(f"| KEY: {key} NOT IN FLIGHTS !")
            return None
        logging.info(f"| KEY: {key} IN FLIGHTS !")
        logging.info(f"| REMOVE DATA FROM SERVER...")
        del self.flights[key]

    def list_actions(self, context):
        return [
            ("clear", "Clear the stored flights."),
            ("shutdown", "Shut down this server."),
            ("remove", "Remove flight from server."),
        ]

    def do_action(self, context, action):
        logging.info(f"| DO ACTION: {action.type} !")
        if action.type == "clear":
            raise NotImplementedError("{} is not implemented.".format(action.type))
        elif action.type == "healthcheck":
            pass
        elif action.type == "shutdown":
            yield pyarrow.flight.Result(pyarrow.py_buffer(b"Shutdown!"))
            # Shut down on background thread to avoid blocking current
            # request
            threading.Thread(target=self._shutdown).start()
        elif action.type == "remove":
            threading.Thread(
                target=self.do_remove_action,
                kwargs={"key": action.body.to_pybytes().decode("utf-8")},
            ).start()
        else:
            raise KeyError("Unknown action {!r}".format(action.type))

    def _shutdown(self):
        """Shut down server."""
        self.shutdown()

    def serve_start(self):
        logging.info(f"| START ARROW FLIGHT SERVING ON:{self.port}")
        self.serve()
