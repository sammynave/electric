defmodule Electric.Postgres.Proxy.SchemaValidationTest do
  use ExUnit.Case, async: true
  alias Electric.DDLX
  alias Electric.Postgres.Proxy.Injector
  alias Electric.Postgres.Proxy.TestScenario
  alias Electric.Postgres.Extension.SchemaLoader
  alias Electric.Postgres.MockSchemaLoader

  import Electric.Postgres.Proxy.TestScenario
  import Electric.Utils, only: [inspect_relation: 1]

  defmodule Simple do
    def name, do: "simple protocol"
    def tag, do: :simple

    def ddl(injector, ddl, tag, state) do
      ddl = String.trim_trailing(ddl)

      injector
      |> client(ddl)
      |> server(complete_ready(tag, state))
    end
  end

  defmodule Extended do
    def name, do: "extended protocol"
    def tag, do: :extended

    def ddl(injector, ddl, tag, state) do
      ddl = String.trim_trailing(ddl)

      injector
      |> client(parse_describe(ddl))
      |> server(parse_describe_complete())
      |> client(bind_execute())
      |> server(bind_execute_complete(tag))
    end
  end

  defp txn(injector) do
    injector
    |> client("BEGIN")
    |> server(complete_ready("BEGIN", :tx))
  end

  defp electrify(injector, name, state \\ :tx) do
    relation = {"public", name}

    injector
    |> client("ALTER TABLE #{inspect_relation(relation)} ENABLE ELECTRIC",
      server: "CALL electric.enable('#{inspect_relation(relation)}');\n"
    )
    |> server(complete_ready("CALL", state),
      client: complete_ready("ELECTRIC ENABLE", state)
    )
  end

  defp create_table(injector, scenario, name, columns, state \\ :tx) do
    relation = {"public", name}
    ddl = "CREATE TABLE #{inspect_relation(relation)} (\n#{Enum.join(columns, ",\n")})"
    scenario.ddl(injector, ddl, "CREATE TABLE", state)
  end

  defp grant_valid(injector, name, permission, state \\ :tx) do
    ddlx = grant(name, permission)
    ddlx_valid(injector, ddlx, state)
  end

  defp grant_error(injector, name, permission, state \\ :tx) do
    ddlx = grant(name, permission)
    ddlx_error(injector, ddlx, state)
  end

  defp ddlx_valid(injector, ddlx, state) do
    command = DDLX.parse!(ddlx)
    sql = DDLX.command_to_postgres(command)

    injector
    |> client(ddlx, server: sql)
    |> server(complete_ready("INSERT", state), client: complete_ready(command.tag, state))
  end

  defp ddlx_error(injector, ddlx, _state) do
    injector
    |> client(ddlx, client: [error(), ready(:failed)])
  end

  # defp _commit(injector) do
  #   injector
  #   |> client("COMMIT", server: capture_version_query())
  #   |> server(complete_ready("CALL", :tx), server: "COMMIT")
  #   |> server(complete_ready("COMMIT", :idle))
  #   |> idle!()
  # end

  defp grant(name, permission) do
    relation = {"public", name}
    "ELECTRIC GRANT #{permission} ON #{inspect_relation(relation)} TO 'some-role'"
  end

  setup do
    # enable all the optional ddlx features
    Electric.Features.process_override(
      proxy_ddlx_grant: true,
      proxy_ddlx_revoke: true,
      proxy_ddlx_assign: true,
      proxy_ddlx_unassign: true,
      proxy_ddlx_sqlite: true
    )

    migrations = [
      {"0001",
       [
         "CREATE TABLE public.truths (id uuid PRIMARY KEY, value text)",
         "CREATE INDEX truths_idx ON public.truths (value)"
       ]}
    ]

    spec = MockSchemaLoader.backend_spec(migrations: migrations)

    {:ok, loader} = SchemaLoader.connect(spec, [])

    {:ok, injector} =
      Injector.new(
        [loader: loader, query_generator: TestScenario.MockInjector],
        username: "electric",
        database: "electric"
      )

    # run inside an explicit tx to make the interaction simpler
    injector = txn(injector)

    %{injector: injector}
  end

  @schemas [
    {"column_unique_constraints",
     [
       "id serial8 PRIMARY KEY",
       "value text NOT NULL",
       "tag text UNIQUE"
     ]}
  ]
  @scenarios [Simple, Extended]

  # this is a complex scenario because the proxy has to keep track of the migrations in-flight but
  # also more realistic
  # describe "granting READ permissions:" do
  #   for scenario <- @scenarios, {name, columns} <- @schemas do
  #     @tag scenario.tag()
  #     @tag String.to_atom(name)
  #     @tag :read
  #     test "#{scenario.name()}: #{String.replace(name, "_", " ")} are permitted", cxt do
  #       cxt.injector
  #       |> create_table(unquote(scenario), unquote(name), unquote(columns))
  #       |> electrify(unquote(name))
  #       |> grant_valid(unquote(name), "READ")
  #     end
  #   end
  # end

  describe "granting WRITE permissions" do
    for scenario <- @scenarios, {name, columns} <- @schemas do
      @tag scenario.tag()
      @tag String.to_atom(name)
      @tag :write
      test "#{scenario.name()}: #{String.replace(name, "_", " ")} are rejected", cxt do
        cxt.injector
        |> create_table(unquote(scenario), unquote(name), unquote(columns))
        |> electrify(unquote(name))
        |> grant_error(unquote(name), "WRITE")
      end
    end

    test "altering table after granting read permissions" do
    end
  end
end
