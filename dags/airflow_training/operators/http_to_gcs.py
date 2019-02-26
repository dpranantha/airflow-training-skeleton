from tempfile import NamedTemporaryFile
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """
    template_fields = ("endpoint", "gcs_path")
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 gcs_bucket,
                 gcs_path,
                 method='GET',
                 http_conn_id='http_default',
                 google_cloud_storage_conn_id="google_cloud_default",
                 *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method
        self.http_conn_id = http_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path

    def execute(self, context):
      # get data from cloud function API
      httphook = HttpHook(method=self.method, http_conn_id=self.http_conn_id)
      response = httphook.run(endpoint=self.endpoint)
      # store date locally in temp file
      with NamedTemporaryFile() as tempfile:
          tempfile.write(response.content)

          #upload to bucket
          gcshook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id)
          gcshook.upload(bucket=self.gcs_bucket, object=self.gcs_path, filename=tempfile.name)

          #remove file
          tempfile.flush()
