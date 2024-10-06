import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag


class TestSpotifyETLPipeline(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder='.', include_examples=False)
        self.dag = self.dagbag.get_dag('spotify_etl_pipeline')

    def test_dag_loaded(self):
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.tasks), 1)
        self.assertEqual(self.dag.task_ids, ['run_etl_task'])

    @patch('etl.extract.extract_new_files')
    @patch('etl.transform.transform_csv')
    @patch('etl.load.load_to_s3')
    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='file1.csv\nfile2.csv')
    def test_run_etl(self, mock_open, mock_load_to_s3, mock_transform_csv, mock_extract_new_files):
        """Test the main ETL function."""
        # Mock the behavior of the extract, transform, and load functions
        mock_extract_new_files.return_value = ['file1.csv', 'file2.csv']
        mock_transform_csv.return_value = MagicMock()  # Mock a DataFrame

        from dags.Spotify_Data_Pipeline import run_etl 

        run_etl()  
      
        mock_extract_new_files.assert_called_once()
        
       
        self.assertEqual(mock_transform_csv.call_count, 2)

  
        self.assertEqual(mock_load_to_s3.call_count, 2)

    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='file1.csv\nfile2.csv')
    def test_update_processed_files(self, mock_open):
        """Test the update of processed files."""
        from dags.Spotify_Data_Pipeline import update_processed_files  

        processed_files = ['file1.csv', 'file2.csv']
        update_processed_files(processed_files)

        mock_open().write.assert_called_once_with('file1.csv\nfile2.csv')


if __name__ == '__main__':
    unittest.main()
