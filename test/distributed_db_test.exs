defmodule DistributedDbTest do
  use ExUnit.Case
  doctest DistributedDb

  test "greets the world" do
    assert DistributedDb.hello() == :world
  end
end
