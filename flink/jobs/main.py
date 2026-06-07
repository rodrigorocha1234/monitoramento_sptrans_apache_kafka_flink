from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration


def main():
    config = Configuration()
    config.set_string("rest.address", "172.20.0.10")
    config.set_integer("rest.port", 8081)

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    ds = env.from_collection(["a", "b", "c"])
    ds.map(lambda x: x.upper()).print()
    print('Teste Remoto')
    env.execute("teste-remoto")


if __name__ == "__main__":
    main()