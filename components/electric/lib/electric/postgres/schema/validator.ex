defmodule Electric.Postgres.Schema.Validator do
  def validate_schema(schema, table, rules) do
    # turn the rules into the %{read?, insert?, update?, delete?, write?} form
    # then validate the table schema for the required perms
  end
end
