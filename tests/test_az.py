"""Tests for the Azure MultiPartUpload class."""

import unittest
from unittest.mock import MagicMock, patch

from odc.geo.cog._az import MultiPartUpload


def test_mpu_init():
    """Basic test for the MultiPartUpload class."""
    account_url = "https://account_name.blob.core.windows.net"
    mpu = MultiPartUpload(account_url, "container", "some.blob", None)
    if mpu.account_url != account_url:
        raise AssertionError(f"mpu.account_url should be '{account_url}'.")
    if mpu.container != "container":
        raise AssertionError("mpu.container should be 'container'.")
    if mpu.blob != "some.blob":
        raise AssertionError("mpu.blob should be 'some.blob'.")
    if mpu.credential is not None:
        raise AssertionError("mpu.credential should be 'None'.")


class TestMultiPartUpload(unittest.TestCase):
    """Test the MultiPartUpload class."""

    @patch("odc.geo.cog._az.BlobServiceClient")
    def test_azure_multipart_upload(self, mock_blob_service_client):
        """Test the MultiPartUpload class."""
        # Arrange - mock the Azure Blob SDK
        # Mock the blob client and its methods
        mock_blob_client = MagicMock()
        mock_container_client = MagicMock()
        mcc = mock_container_client
        mock_blob_service_client.return_value.get_container_client.return_value = mcc
        mock_container_client.get_blob_client.return_value = mock_blob_client

        # Simulate return values for Azure Blob SDK methods
        mock_blob_client.get_blob_properties.return_value.etag = "mock-etag"

        # Test parameters
        account_url = "https://mockaccount.blob.core.windows.net"
        container = "mock-container"
        blob = "mock-blob"
        credential = "mock-sas-token"

        # Act - create an instance of MultiPartUpload and call its methods
        azure_upload = MultiPartUpload(account_url, container, blob, credential)
        upload_id = azure_upload.initiate()
        part1 = azure_upload.write_part(1, b"first chunk of data")
        part2 = azure_upload.write_part(2, b"second chunk of data")
        etag = azure_upload.finalise([part1, part2])

        # Assert - check the results
        # Check that the initiate method behaves as expected
        self.assertEqual(upload_id, "azure-block-upload")

        # Verify the calls to Azure Blob SDK methods
        mock_blob_service_client.assert_called_once_with(
            account_url=account_url, credential=credential
        )
        mock_blob_client.stage_block.assert_any_call(
            part1["BlockId"], b"first chunk of data"
        )
        mock_blob_client.stage_block.assert_any_call(
            part2["BlockId"], b"second chunk of data"
        )
        mock_blob_client.commit_block_list.assert_called_once()
        self.assertEqual(etag, "mock-etag")

        # Verify block list passed during finalise
        block_list = mock_blob_client.commit_block_list.call_args[0][0]
        self.assertEqual(len(block_list), 2)
        self.assertEqual(block_list[0].id, part1["BlockId"])
        self.assertEqual(block_list[1].id, part2["BlockId"])
