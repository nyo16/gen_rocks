defmodule GenRocksTest do
  use ExUnit.Case
  doctest GenRocks

  test "greets the world" do
    assert GenRocks.hello() == :world
  end
end
