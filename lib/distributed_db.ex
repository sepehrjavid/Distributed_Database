defmodule DistributedDb do
  @moduledoc """
  Documentation for `DistributedDb`.
  """

  @doc """
  Hello world.

  ## Examples

      iex> DistributedDb.hello()
      :world

  """
  def hello do
    Node.connect(:"manager@192.168.1.13")
  end
end
