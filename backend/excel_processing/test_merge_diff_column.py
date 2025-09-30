import pytest
import pandas as pd
from excel_processing.merge_diff_column import find_org_differences


class TestFindOrgDifferences:

    def test_no_differences(self):
        """测试当两列完全相同时返回None"""
        reference = pd.Series(['A', 'B', 'C'])
        current = pd.Series(['A', 'B', 'C'])

        result = find_org_differences(reference, current)
        assert result is None

    def test_all_differences(self):
        """测试当两列完全不同时返回所有差异行"""
        reference = pd.Series(['A', 'B', 'C'])
        current = pd.Series(['X', 'Y', 'Z'])

        expected = pd.DataFrame({
            'reference': ['A', 'B', 'C'],
            'current': ['X', 'Y', 'Z']
        })

        result = find_org_differences(reference, current)
        pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)

    def test_some_differences(self):
        """测试当两列部分不同时返回差异行"""
        reference = pd.Series(['A', 'B', 'C', 'D'])
        current = pd.Series(['A', 'X', 'C', 'Y'])

        expected = pd.DataFrame(
            {
                'reference': ['B', 'D'],
                'current': ['X', 'Y']
            }, index=[1, 3])

        result = find_org_differences(reference, current)
        pd.testing.assert_frame_equal(result, expected)

    def test_empty_input(self):
        """测试空输入时返回None"""
        reference = pd.Series([], dtype=str)
        current = pd.Series([], dtype=str)

        result = find_org_differences(reference, current)
        assert result is None

    def test_nan_values(self):
        """测试包含NaN值的比较"""
        reference = pd.Series(['A', None, 'C'])
        current = pd.Series(['A', 'B', None])

        expected = pd.DataFrame(
            {
                'reference': [None, 'C'],
                'current': ['B', None]
            }, index=[1, 2])

        result = find_org_differences(reference, current)
        pd.testing.assert_frame_equal(result, expected)
