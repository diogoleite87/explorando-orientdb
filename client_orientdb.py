import pyorient


class OrientDBClient:
    def __init__(self, host, port) -> None:
        self.client = pyorient.OrientDB(host, port)

    def connect_user(self, username, password):
        self.client.connect(username, password)

    def list_database(self):
        print(self.client.db_list())

    def create_database(self, db_name):

        if not self.client.db_exists(db_name):
            self.client.db_create(name=db_name)
        else:
            print(f'Banco de dados com o nome: {db_name} ja existe.')

    def open_database(self, db_name, user, password):
        self.client.db_open(db_name=db_name, user=user, password=password)

    def query_db(self, query):
        return self.client.query(query)

    def command_db(self, command):
        return self.client.command(command)
