import uuid
from pathlib import Path
from typing import List

import pika
from semver import Version

from climatoology.app.plugin import PlatformPlugin
from climatoology.base.operator import Operator, Info, Artifact, ArtifactModality
from climatoology.store.object_store import Storage

if __name__ == '__main__':
    class SampleOperator(Operator):

        def info(self) -> Info:
            return Info('ghg_lulc', Version(0, 0, 1), 'purpose', 'methods')

        def report(self, params: dict) -> List[Artifact]:
            return [Artifact(uuid.uuid4(), ArtifactModality.MAP_LAYER, file_path=Path('../climatoology'))]


    class SampleStorage(Storage):

        def save(self, artifact: Artifact) -> uuid.UUID:
            pass

        def save_all(self, artifacts: List[Artifact]) -> List[uuid.UUID]:
            pass


    plugin = PlatformPlugin(SampleOperator(), SampleStorage(),
                            pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672)))
    plugin.run()
