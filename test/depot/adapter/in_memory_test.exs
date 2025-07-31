defmodule Depot.Adapter.InMemoryTest do
  use ExUnit.Case, async: true
  import Depot.AdapterTest
  doctest Depot.Adapter.InMemory

  adapter_test %{test: test} do
    filesystem = Depot.Adapter.InMemory.configure(name: test)
    start_supervised(filesystem)
    {:ok, filesystem: filesystem}
  end

  describe "write" do
    test "success", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok = Depot.Adapter.InMemory.write(config, "test.txt", "Hello World", [])

      assert {:ok, {"Hello World", _meta}} =
               Agent.get(via(test), fn state ->
                 state
                 |> elem(0)
                 |> Map.fetch!("/")
                 |> elem(0)
                 |> Map.fetch("test.txt")
               end)
    end

    test "folders are automatically created is missing", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok = Depot.Adapter.InMemory.write(config, "folder/test.txt", "Hello World", [])

      assert {:ok, "Hello World"} = Depot.Adapter.InMemory.read(config, "folder/test.txt")
    end
  end

  describe "read" do
    test "success", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      assert {:ok, "Hello World"} = Depot.Adapter.InMemory.read(config, "test.txt")
    end

    test "stream success", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      assert {:ok, %Depot.Adapter.InMemory.AgentStream{} = stream} =
               Depot.Adapter.InMemory.read_stream(config, "test.txt", [])

      assert Enum.into(stream, <<>>) == "Hello World"
    end

    test "stream with custom chunk size", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      assert {:ok, stream} =
               Depot.Adapter.InMemory.read_stream(config, "test.txt", chunk_size: 3)

      chunks = Enum.to_list(stream)
      assert length(chunks) > 1
      assert Enum.join(chunks) == "Hello World"
    end

    test "stream enumerable protocol count/1 fallback works", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      {:ok, stream} = Depot.Adapter.InMemory.read_stream(config, "test.txt", chunk_size: 5)
      # Enum.count/1 falls back to reduce when count/1 returns error
      assert Enum.count(stream) > 0
    end

    test "stream enumerable protocol slice/1 fallback works", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      {:ok, stream} = Depot.Adapter.InMemory.read_stream(config, "test.txt", chunk_size: 5)
      # Enum.slice/3 falls back to reduce when slice/1 returns error
      result = Enum.slice(stream, 0, 1)
      assert is_list(result)
    end

    test "stream enumerable protocol member?/2 fallback works", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello", %{}}}, %{}}}, %{}}
        end)

      {:ok, stream} = Depot.Adapter.InMemory.read_stream(config, "test.txt", [])
      # Enum.member?/2 falls back to reduce when member?/2 returns error
      assert Enum.member?(stream, "Hello") == true
    end

    test "stream for non-existent file returns empty", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      assert {:ok, stream} =
               Depot.Adapter.InMemory.read_stream(config, "missing.txt", [])

      assert Enum.to_list(stream) == []
    end

    test "stream suspend and resume functionality", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      {:ok, stream} = Depot.Adapter.InMemory.read_stream(config, "test.txt", chunk_size: 2)

      # Test suspend/resume by taking only first 2 chunks
      result = Enum.take(stream, 2)
      assert length(result) == 2
      assert is_binary(hd(result))
    end
  end

  describe "write_stream" do
    test "collectable protocol writes data correctly", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      {:ok, stream} = Depot.Adapter.InMemory.write_stream(config, "output.txt", [])

      data = ["Hello", " ", "World"]
      result_stream = Enum.into(data, stream)

      assert result_stream.path == "output.txt"
      assert {:ok, "Hello World"} = Depot.Adapter.InMemory.read(config, "output.txt")
    end

    test "collectable protocol appends to existing file", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      # Write initial content
      :ok = Depot.Adapter.InMemory.write(config, "append.txt", "Initial ", [])

      {:ok, stream} = Depot.Adapter.InMemory.write_stream(config, "append.txt", [])

      data = ["appended", " content"]
      Enum.into(data, stream)

      assert {:ok, "Initial appended content"} = Depot.Adapter.InMemory.read(config, "append.txt")
    end

    test "collectable protocol handles halt", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      {:ok, stream} = Depot.Adapter.InMemory.write_stream(config, "halt.txt", [])

      # Simulate halt by accessing the collector function directly
      {[], collector_fun} = Collectable.into(stream)
      result = collector_fun.([], :halt)

      assert result == :ok
    end
  end

  describe "delete" do
    test "success", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      :ok =
        Agent.update(via(test), fn _state ->
          {%{"/" => {%{"test.txt" => {"Hello World", %{}}}, %{}}}, %{}}
        end)

      assert :ok = Depot.Adapter.InMemory.delete(config, "test.txt")

      assert :error =
               Agent.get(via(test), fn state ->
                 state
                 |> elem(0)
                 |> Map.fetch!("/")
                 |> elem(0)
                 |> Map.fetch("test.txt")
               end)
    end

    test "successful even if no file to delete", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)

      start_supervised(filesystem)

      assert :ok = Depot.Adapter.InMemory.delete(config, "test.txt")
    end
  end

  describe "versioning" do
    test "write_version creates version and returns version_id", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      assert {:ok, version_id} =
               Depot.Adapter.InMemory.write_version(config, "test.txt", "Hello World v1", [])

      assert is_binary(version_id)
      assert String.length(version_id) == 32
    end

    test "read_version retrieves specific version", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, version_id} =
        Depot.Adapter.InMemory.write_version(config, "test.txt", "Hello World v1", [])

      assert {:ok, "Hello World v1"} =
               Depot.Adapter.InMemory.read_version(config, "test.txt", version_id)
    end

    test "list_versions returns all versions for a path", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, v1} = Depot.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, v2} = Depot.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      {:ok, versions} = Depot.Adapter.InMemory.list_versions(config, "test.txt")
      assert length(versions) == 2
      assert Enum.any?(versions, &(&1.version_id == v1))
      assert Enum.any?(versions, &(&1.version_id == v2))
    end

    test "get_latest_version returns most recent version", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, _v1} = Depot.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, v2} = Depot.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      assert {:ok, ^v2} = Depot.Adapter.InMemory.get_latest_version(config, "test.txt")
    end

    test "restore_version restores file to specific version", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, v1} = Depot.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, _v2} = Depot.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      assert :ok = Depot.Adapter.InMemory.restore_version(config, "test.txt", v1)
      assert {:ok, "Version 1"} = Depot.Adapter.InMemory.read(config, "test.txt")
    end

    test "delete_version removes specific version", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, v1} = Depot.Adapter.InMemory.write_version(config, "test.txt", "Version 1", [])
      {:ok, v2} = Depot.Adapter.InMemory.write_version(config, "test.txt", "Version 2", [])

      assert :ok = Depot.Adapter.InMemory.delete_version(config, "test.txt", v1)

      {:ok, versions} = Depot.Adapter.InMemory.list_versions(config, "test.txt")
      assert length(versions) == 1
      assert hd(versions).version_id == v2

      assert {:error, _} = Depot.Adapter.InMemory.read_version(config, "test.txt", v1)
    end

    test "versioning preserves visibility", %{test: test} do
      {_, config} = filesystem = Depot.Adapter.InMemory.configure(name: test)
      start_supervised(filesystem)

      {:ok, version_id} =
        Depot.Adapter.InMemory.write_version(config, "test.txt", "Content", visibility: :public)

      assert :ok = Depot.Adapter.InMemory.restore_version(config, "test.txt", version_id)
      assert {:ok, :public} = Depot.Adapter.InMemory.visibility(config, "test.txt")
    end
  end

  defp via(name) do
    Depot.Registry.via(Depot.Adapter.InMemory, name)
  end
end
