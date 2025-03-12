from pathlib import Path
import uuid

from pyoxigraph import Store, Quad, NamedNode, Literal


class Repository:
    def __init__(
        self,
        data_directory: str | Path = "~/.mollusk",
    ):
        self.data_directory = self._prepare_data_directory(data_directory)
        self.graphdb = GraphDB(self.data_directory)

    def _prepare_data_directory(self, data_directory: str | Path) -> Path:
        data_directory = Path(data_directory).expanduser()
        if not data_directory.exists():
            data_directory.mkdir()
        return data_directory


class GraphDB:
    def __init__(self, data_directory: str | Path):
        self.data_directory = data_directory
        self.store = self.get_store()

    def get_store(self):
        return Store(path=self.data_directory / "oxigraph")

    def add_statement(
        self,
        subj: Literal | NamedNode,
        pred: Literal | NamedNode,
        obj: Literal | NamedNode,
        graph_name=None,
    ):
        quad = Quad(subj, pred, obj, graph_name)
        self.store.add(quad)

    def sparql(self, query_string):
        return self.store.query(query_string)


class Item:
    def __init__(
        self,
        item_uuid: str | None = None,
        title: str | None = None,
    ):
        self.item_uuid: str = item_uuid or str(uuid.uuid4())
        self.title: str = title or "No Title"


repo = Repository()

"""
repo.graphdb.add_statement(
    subj=NamedNode('http://henondesigns.github.io/coffee/lobby'),
    pred=NamedNode('http://henondesigns.github.io/flavor'),
    obj=Literal('medium')
)

resp = repo.graphdb.sparql('select ?subj ?pred ?obj where {?subj ?pred ?obj .}')
list(resp)
"""


def simulate_n_writes(repo: Repository, n=100):
    import time

    t0 = time.time()
    for x in range(n):
        repo.graphdb.add_statement(
            subj=NamedNode(f"http://henondesigns.github.io/number/{x}"),
            pred=NamedNode("http://henondesigns.github.io/enjoysUuid"),
            obj=Literal(str(uuid.uuid4())),
        )
    print(f"elapsed: {time.time() - t0}")
