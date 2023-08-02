from pathlib import Path

from semver import Version

from climatoology.base.operator import Info


def test_operator_info():
    description = Info(
        name='test_plugin',
        version=Version.parse('3.1.0'),
        purpose='The purpose of this base is to '
                'present basic library properties in '
                'terms of enforcing similar capabilities '
                'between Climate Action event components',
        methodology='This is a test base',
        sources_bib=Path('base/test.bib').absolute()
    )
    assert description.version == '3.1.0'
    assert description.sources['test2023']['ENTRYTYPE'] == 'article'
