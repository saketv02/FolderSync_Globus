from globus_sdk import TransferClient

class GridFTPConnection:

    def __init__(self,endpoint_id,source_id):
        self.transfer_client = TransferClient()
        self.endpoint_id = endpoint_id
        self.source_id = source_id
        self.endpoint = self.transfer_client.get_endpoint(endpoint_id)
        print("Connection successful to:", self.endpoint["display_name"] or self.endpoint["canonical_name"])