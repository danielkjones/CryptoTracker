from test.helpers import TestConstants as tc

from src.steps.display_averages import DisplayAveragesStep


class TestDisplayAverages:

    def test_hello_world(self):
        """This doesn't have any true assertions, but helpful for spot checking in debug console"""
        step = DisplayAveragesStep(tc.TEST_TIMESTAMP)
        step.avg_bitcoin_diff_directory = tc.MOCK_AVG_BITCOIN_DIFF_DIRECTORY
        step.display_averages()
