"""Tests for the Azure AzMultiPartUpload class."""

import base64
import unittest
from unittest.mock import MagicMock, patch

# Conditional import for Azure support
try:
    from odc.geo.cog._az import AzMultiPartUpload

    HAVE_AZURE = True
except ImportError:
    AzMultiPartUpload = None
    HAVE_AZURE = False


def require_azure(test_func):
    """Decorator to skip tests if Azure dependencies are not installed."""
    return unittest.skipUnless(HAVE_AZURE, "Azure dependencies are not installed")(
        test_func
    )


class TestAzMultiPartUpload(unittest.TestCase):
    """Test the AzMultiPartUpload class."""

    @require_azure
    def test_mpu_init(self):
        """Basic test for AzMultiPartUpload initialization."""
        account_url = "https://account_name.blob.core.windows.net"
        mpu = AzMultiPartUpload(account_url, "container", "some.blob", None)

        self.assertEqual(mpu.account_url, account_url)
        self.assertEqual(mpu.container, "container")
        self.assertEqual(mpu.blob, "some.blob")
        self.assertIsNone(mpu.credential)

    @require_azure
    @patch("odc.geo.cog._az.BlobServiceClient")
    def test_azure_multipart_upload(self, mock_blob_service_client):
        """Test the full Azure AzMultiPartUpload functionality."""
        # Arrange - Mock Azure Blob SDK client structure
        mock_blob_client = MagicMock()
        mock_container_client = MagicMock()
        mock_blob_service_client.return_value.get_container_client.return_value = (
            mock_container_client
        )
        mock_container_client.get_blob_client.return_value = mock_blob_client

        # Simulate return values for Azure Blob SDK methods
        mock_blob_client.get_blob_properties.return_value.etag = "mock-etag"

        # Test parameters
        account_url = "https://mockaccount.blob.core.windows.net"
        container = "mock-container"
        blob = "mock-blob"
        credential = "mock-sas-token"

        # Act
        azure_upload = AzMultiPartUpload(account_url, container, blob, credential)
        upload_id = azure_upload.initiate()
        part1 = azure_upload.write_part(1, b"first chunk of data")
        part2 = azure_upload.write_part(2, b"second chunk of data")
        etag = azure_upload.finalise([part1, part2])

        # Correctly calculate block IDs
        block_id1 = base64.b64encode(b"block-1").decode("utf-8")
        block_id2 = base64.b64encode(b"block-2").decode("utf-8")

        # Assert
        self.assertEqual(upload_id, "azure-block-upload")
        self.assertEqual(etag, "mock-etag")

        # Verify BlobServiceClient instantiation
        mock_blob_service_client.assert_called_once_with(
            account_url=account_url, credential=credential
        )

        # Verify stage_block calls
        mock_blob_client.stage_block.assert_any_call(
            block_id=block_id1, data=b"first chunk of data"
        )
        mock_blob_client.stage_block.assert_any_call(
            block_id=block_id2, data=b"second chunk of data"
        )

        # Verify commit_block_list was called correctly
        block_list = mock_blob_client.commit_block_list.call_args[0][0]
        self.assertEqual(len(block_list), 2)
        self.assertEqual(block_list[0].id, block_id1)
        self.assertEqual(block_list[1].id, block_id2)
        mock_blob_client.commit_block_list.assert_called_once()


if __name__ == "__main__":
    unittest.main()
