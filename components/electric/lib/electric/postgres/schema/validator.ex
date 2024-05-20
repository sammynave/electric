defmodule Electric.Postgres.Schema.Validator do
  alias Electric.Postgres.Schema
  alias Electric.Satellite.SatPerms

  @write_privs Electric.Satellite.Permissions.write_privileges()

  def validate_schema_for_permissions(schema, %{grants: grants}) do
    # turn the rules into the %{read?, insert?, update?, delete?, write?} form
    # then validate the table schema for the required perms
    Enum.reduce_while(grants, {:ok, []}, &validate_grant(&1, &2, schema))
  end

  defp validate_grant(%SatPerms.Grant{privilege: privilege} = grant, {:ok, warnings}, schema) do
    %{table: %{schema: sname, name: name}} = grant
    table_schema = Schema.fetch_table!(schema, {sname, name})
    validate_table_for(table_schema, privilege, warnings)
  end

  defp validate_table_for(table_schema, privilege, warnings) when privilege in @write_privs do
    with {:ok, warnings} <- validate_write_constraints(table_schema, warnings) do
      {:cont, {:ok, warnings}}
    else
      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp validate_table_for(_table_schema, _privilege, warnings) do
    {:cont, {:ok, warnings}}
  end

  @allowed_write_constraints [:primary, :not_null, :foreign]

  defp validate_write_constraints(table_schema, warnings) do
    Enum.reduce_while(table_schema.constraints, {:ok, warnings}, fn
      %{constraint: {c, _}}, {:ok, warnings} when c in @allowed_write_constraints ->
        {:cont, {:ok, warnings}}

      %{constraint: {:unique, unique}}, {:ok, _warnings} ->
        dbg(unique)
        {:halt, {:error, table_schema.name, "unique constraint!"}}
    end)
  end
end
