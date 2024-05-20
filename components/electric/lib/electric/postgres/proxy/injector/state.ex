defmodule Electric.Postgres.Proxy.Injector.State do
  alias PgProtocol.Message, as: M
  alias Electric.Postgres
  alias Electric.Postgres.Extension.SchemaLoader

  defmodule Tx do
    alias Electric.Satellite.SatPerms
    @moduledoc false
    # holds information about the current transaction

    defstruct electrified: false,
              version: nil,
              tables: %{},
              rules: nil,
              schema: nil,
              stmts: %{},
              binds: %{}

    @type t() :: %__MODULE__{
            electrified: boolean(),
            version: nil | String.t(),
            rules: nil | %SatPerms.Rules{},
            schema: nil | Postgres.Schema.t(),
            stmts: %{String.t() => String.t()},
            binds: %{String.t() => String.t()}
          }

    def new(loader) do
      {:ok, rules} = SchemaLoader.global_permissions(loader)
      # TODO: this schema version could be inconsistent with the database
      # there could be a migration in the replication stream that hasn't reached
      # our state maintenance consumer (MigrationConsumer)
      # Perhaps we could move the schema mutation/update to within the proxy itself
      # and provide a way to retrieve based on txid or something
      {:ok, schema_version} = SchemaLoader.load(loader)

      %__MODULE__{rules: rules, schema: schema_version.schema}
    end

    def electrify_table(tx, {schema, table}) do
      Map.update!(tx, :tables, &Map.put(&1, {schema, table}, true))
    end

    def table_electrified?(tx, {schema, table}) do
      Map.get(tx.tables, {schema, table}, false)
    end
  end

  @derive {Inspect, except: [:loader]}

  defstruct loader: nil,
            query_generator: nil,
            default_schema: "public",
            tx: nil,
            session_id: nil,
            metadata: %{},
            pending_messages: []

  @type loader() :: {module(), term()}
  @type query_generator() :: module()
  @type t() :: %__MODULE__{
          loader: loader(),
          query_generator: query_generator(),
          default_schema: String.t(),
          session_id: integer(),
          tx: nil | Tx.t(),
          metadata: map(),
          pending_messages: [M.t()]
        }

  @doc """
  Set the current state as being inside a transaction.
  """
  @spec begin(t()) :: t()
  def begin(%__MODULE__{} = state) do
    %{state | tx: Tx.new(state.loader)}
  end

  @doc """
  Exit the current transaction.
  """
  @spec commit(t()) :: t()
  def commit(%__MODULE__{} = state) do
    %{state | tx: nil}
  end

  @doc """
  Exit the current transaction.
  """
  @spec rollback(t()) :: t()
  def rollback(%__MODULE__{} = state) do
    %{state | tx: nil}
  end

  @doc """
  Are we in a transaction or not?
  """
  @spec tx?(t()) :: boolean()
  def tx?(%__MODULE__{} = state) do
    not is_nil(state.tx)
  end

  @doc """
  Update the transaction status to mark it as affecting electrified tables (or
  not).
  """
  @spec electrify(t()) :: t()
  def electrify(%__MODULE__{} = state) do
    maybe_update_tx(state, &Map.put(&1, :electrified, true))
  end

  @spec electrify(t(), {String.t(), String.t()}) :: t()
  def electrify(%__MODULE__{} = state, {_schema, _name} = table) do
    state
    |> electrify()
    |> maybe_update_tx(&Tx.electrify_table(&1, table))
  end

  def electrified?(%__MODULE__{tx: %Tx{electrified: electrified?}}), do: electrified?
  def electrified?(%__MODULE__{}), do: false

  defp maybe_update_tx(state, update_fun) do
    Map.update!(state, :tx, fn
      nil -> nil
      tx when is_map(tx) -> update_fun.(tx)
    end)
  end

  @doc """
  Wrapper around the SchemaLoader.table_electrified?/2 behaviour callback.
  """
  @spec table_electrified?(t(), {String.t(), String.t()}) :: boolean()
  def table_electrified?(%__MODULE__{loader: {module, conn}} = state, table) do
    if tx?(state) && Tx.table_electrified?(state.tx, table) do
      true
    else
      {:ok, electrified?} = apply(module, :table_electrified?, [conn, table])
      electrified?
    end
  end

  @doc """
  Wrapper around the SchemaLoader.index_electrified?/2 behaviour callback.
  """
  @spec index_electrified?(t(), {String.t(), String.t()}) :: {:ok, boolean()}
  def index_electrified?(%__MODULE__{loader: {module, conn}}, index) do
    {:ok, electrified?} = apply(module, :index_electrified?, [conn, index])
    electrified?
  end

  @doc """
  Retrieve the migration version assigned to the current transaction.

  Returns `:error` if we're outside a transaction or no version has been
  assigned.
  """
  @spec tx_version(t) :: {:ok, String.t()} | :error
  def tx_version(%__MODULE__{tx: %Tx{version: nil}}) do
    :error
  end

  def tx_version(%__MODULE__{tx: %Tx{version: version}}) do
    {:ok, version}
  end

  def tx_version(%__MODULE__{}) do
    :error
  end

  def permissions_rules(%__MODULE__{tx: nil, loader: loader}) do
    SchemaLoader.global_permissions(loader)
  end

  def permissions_rules(%__MODULE__{tx: %{rules: rules}}) do
    {:ok, rules}
  end

  @doc """
  Assign a version to the current transaction.
  """
  @spec tx_version(t(), integer() | String.t()) :: t()
  def tx_version(%__MODULE__{} = state, version) do
    maybe_update_tx(state, &Map.put(&1, :version, to_string(version)))
  end

  @doc """
  Returns true if the state has an assigned migration version.
  """
  @spec tx_version?(t()) :: boolean()
  def tx_version?(%__MODULE__{} = state) do
    case tx_version(state) do
      {:ok, _version} -> true
      :error -> false
    end
  end

  def assign_version_metadata(%__MODULE__{} = state, version) do
    Map.update!(state, :metadata, &Map.put(&1, :version, to_string(version)))
  end

  def retrieve_version_metadata(%__MODULE__{} = state) do
    Map.get_and_update!(state, :metadata, fn m ->
      {Map.fetch(m, :version), Map.delete(m, :version)}
    end)
  end

  def mutate_schema(%__MODULE__{tx: nil} = state, _msgs) do
    state
  end

  def mutate_schema(%__MODULE__{tx: tx} = state, msgs) do
    tx = Enum.reduce(msgs, tx, &apply_msgs_tx/2)
    # dbg(tx)
    %{state | tx: tx}
  end

  defp apply_msgs_tx(%M.Parse{} = msg, tx) do
    Map.update!(tx, :stmts, &Map.put(&1, msg.name, msg.query))
  end

  defp apply_msgs_tx(%M.Query{} = msg, tx) do
    update_schema(msg.query, tx)
  end

  defp apply_msgs_tx(%M.Bind{} = msg, tx) do
    Map.update!(tx, :binds, &Map.put(&1, msg.portal, msg.source))
  end

  defp apply_msgs_tx(%M.Execute{} = msg, tx) do
    {name, binds} = Map.pop!(tx.binds, msg.portal)
    {stmt, stmts} = Map.pop!(tx.stmts, name)

    update_schema(stmt, %{tx | binds: binds, stmts: stmts})
  end

  defp apply_msgs_tx(_msg, tx) do
    tx
  end

  defp update_schema(stmt, tx) do
    schema = Postgres.Schema.update(tx.schema, stmt, oid_loader: &oid_loader/3)

    %{tx | schema: schema}
  end

  # we don't need real oids
  defp oid_loader(type, schema, name) do
    {:ok, Enum.join(["#{type}", schema, name], ".") |> :erlang.phash2(50_000)}
  end

  def schema!(%__MODULE__{tx: nil}) do
    raise "Not in transaction"
  end

  def schema!(%__MODULE__{tx: tx}) do
    tx.schema
  end
end
