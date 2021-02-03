defmodule Peer do
  @manager_name :chord_manager
  def start() do
    spawn(__MODULE__, :join, [])
  end

  def join() do
    send(:global.whereis_name(@manager_name), {:join, self()})
    receive do
      {:reject} ->
        IO.puts("Oops, the network seems full!")
        exit(self())
      {:ok, node_numbers, id, successor_data, predecessor_data, holding_data} ->
        IO.puts("Obtained ID: #{id}")
        run(node_numbers, id, successor_data, predecessor_data, holding_data)
    end
  end

  def run(node_numbers, id, nil, nil, holding_data) do
    finger_updater_pid = spawn(__MODULE__, :finger_update_manager, [self(), node_numbers, id, nil, nil])
    IO.puts("Currently holding #{inspect holding_data} as data")
    IO.puts("I'm all alone in the network now :( but it's cool that I don't need to update my finger table")
    loop([], node_numbers, id, nil, nil, finger_updater_pid, holding_data)
  end
  def run(node_numbers, id, successor_data, predecessor_data, nil) do
    IO.puts("Obtained Successor ID #{elem(successor_data, 0)}")
    IO.puts("Obtained Predecessor ID #{elem(predecessor_data, 0)}")
    receive do
      {:recieve_own_data, holding_data} ->
        IO.puts("Currently holding #{inspect holding_data} as data")
        finger_updater_pid = spawn(__MODULE__, :finger_update_manager, [self(), node_numbers, id, successor_data, predecessor_data])
        loop([successor_data], node_numbers, id, successor_data, predecessor_data, finger_updater_pid, holding_data)
    end
  end

  def get_own_data(node_numbers, _, nil) do
    Enum.to_list(0..node_numbers - 1)
  end
  def get_own_data(node_numbers, id, predecessor_data) do
    diff = id - elem(predecessor_data, 0)
    cond do
      diff < 0 ->
        temp_id = id + node_numbers
        temp_data = Enum.to_list(elem(predecessor_data, 0) + 1..temp_id)
        Enum.map(temp_data, fn x -> rem(x, node_numbers) end)
      true -> Enum.to_list(elem(predecessor_data, 0) + 1..id)
    end
  end

  def finger_update_manager(loop_pid, node_numbers, id, nil, nil) do
    receive do
      {:stop} -> exit(self())
      {:update_successor, new_successor_data} -> finger_update_manager(loop_pid, node_numbers, id, new_successor_data, nil)
      {:update_predecessor, new_predecessor_data} -> finger_update_manager(loop_pid, node_numbers, id, nil, new_predecessor_data)
    end
  end
  def finger_update_manager(loop_pid, node_numbers, id, successor_data, nil) do
    receive do
      {:stop} -> exit(self())
      {:update_successor, new_successor_data} -> finger_update_manager(loop_pid, node_numbers, id, new_successor_data, nil)
      {:update_predecessor, new_predecessor_data} -> finger_update_manager(loop_pid, node_numbers, id, successor_data, new_predecessor_data)
    end
  end
  def finger_update_manager(loop_pid, node_numbers, id, nil, predecessor_data) do
    receive do
      {:stop} -> exit(self())
      {:update_successor, new_successor_data} -> finger_update_manager(loop_pid, node_numbers, id, new_successor_data, predecessor_data)
      {:update_predecessor, new_predecessor_data} -> finger_update_manager(loop_pid, node_numbers, id, nil, new_predecessor_data)
    end
  end
  def finger_update_manager(loop_pid, node_numbers, id, successor_data, predecessor_data) do
    receive do
      {:stop} -> exit(self())
      {:update_successor, new_successor_data} -> finger_update_manager(loop_pid, node_numbers, id, new_successor_data, predecessor_data)
      {:update_predecessor, new_predecessor_data} -> finger_update_manager(loop_pid, node_numbers, id, successor_data, new_predecessor_data)
    after 6000 ->
      IO.puts("Finger Table Update Starts")
      update_finger_table(loop_pid, node_numbers, id, successor_data, predecessor_data)
      finger_update_manager(loop_pid, node_numbers, id, successor_data, predecessor_data)
    end
  end

  def form_finger_table(current_finger_table, node_numbers, id, successor_data, predecessor_data, i) when Bitwise.<<<(1, i) < node_numbers do
    new_id = rem(id + Bitwise.<<<(1, i), node_numbers)
    peer_holding_data = get_own_data(node_numbers, id, predecessor_data)
    cond do
      new_id in peer_holding_data -> form_finger_table(current_finger_table, node_numbers, id, successor_data, predecessor_data, i + 1)
      true ->
        send(elem(successor_data, 1), {:find_key, new_id, self(), [id]})
        receive do
          {:found_id, peer_data, _} -> form_finger_table([peer_data | current_finger_table], node_numbers, id, successor_data, predecessor_data, i + 1)
          {:not_found} -> form_finger_table(current_finger_table, node_numbers, id, successor_data, predecessor_data, i + 1)
        after 2000 -> form_finger_table(current_finger_table, node_numbers, id, successor_data, predecessor_data, i + 1)
        end
    end
  end
  def form_finger_table(current_finger_table, _, _, _, _, _) do
    current_finger_table
  end

  def update_finger_table(loop_pid, node_numbers, id, successor_data, predecessor_data) do
    new_finger_table = [successor_data]
    new_finger_table = form_finger_table(new_finger_table, node_numbers, id, successor_data, predecessor_data, 1)
    new_finger_table = Enum.uniq(Enum.reverse(new_finger_table))
    IO.puts("Finger Table Update Done")
    send(loop_pid, {:update_finger_table, new_finger_table})
  end

  def normalize_known_ids(_, [], searching_id, node_numbers, acc, true) do
    {Enum.reverse(acc), searching_id + node_numbers}
  end
  def normalize_known_ids(_, [], searching_id, _, acc, false) do
    {Enum.reverse(acc), searching_id}
  end
  def normalize_known_ids(prev, [head|tail], searching_id, node_numbers, acc, _) when prev > head do
    normalize_known_ids(head + node_numbers, tail, searching_id, node_numbers, [head + node_numbers | acc], true)
  end
  def normalize_known_ids(_, [head|tail], searching_id, node_numbers, acc, flag) do
    normalize_known_ids(head, tail, searching_id, node_numbers, [head | acc], flag)
  end

  def find_next_hop(nil, [head | _], normalized_searching_id) when normalized_searching_id <= head do
    head
  end
  def find_next_hop(prev, [], _) do
    prev
  end
  def find_next_hop(prev, [head | _], normalized_searching_id) when normalized_searching_id <= head and normalized_searching_id > prev do
    head
  end
  def find_next_hop(_, [head | tail], normalized_searching_id) do
    find_next_hop(head, tail, normalized_searching_id)
  end

  def find_path_to_id(known_ids, searching_id, node_numbers) do
    {normalized_keys, normalized_searching_id} = normalize_known_ids(hd(known_ids), tl(known_ids), searching_id, node_numbers, [hd(known_ids)], false)
    find_next_hop(nil, normalized_keys, normalized_searching_id)
  end

  def find_key(finger_table, target_key, pid, id, node_numbers, predecessor_data, loop_pid, nil, holding_data) do
    peer_holding_data = get_own_data(node_numbers, id, predecessor_data)
    finger_keys_list = Enum.map(finger_table, fn x -> elem(x, 0) end)
    cond do
      target_key in peer_holding_data -> validate_with_holding_data(pid, id, loop_pid, holding_data, target_key)
      length(finger_keys_list) == 0 -> send(pid, {:not_found})
      true ->
        next_hop_id = rem(find_path_to_id(finger_keys_list, target_key, node_numbers), node_numbers)
        next_hop_data = Enum.filter(finger_table, fn x -> elem(x, 0) == next_hop_id end)
        send(elem(hd(next_hop_data), 1), {:find_key, target_key, pid, [id]})
    end
  end
  def find_key(finger_table, target_key, pid, id, node_numbers, predecessor_data, loop_pid, source_ids, holding_data) do
    peer_holding_data = get_own_data(node_numbers, id, predecessor_data)
    finger_keys_list = Enum.filter(Enum.map(finger_table, fn x -> elem(x, 0) end), fn x -> x not in source_ids end)
    cond do
      target_key in peer_holding_data -> validate_with_holding_data(pid, id, loop_pid, holding_data, target_key)
      length(finger_keys_list) == 0 -> send(pid, {:not_found})
      true ->
        next_hop_id = rem(find_path_to_id(finger_keys_list, target_key, node_numbers), node_numbers)
        next_hop_data = Enum.filter(finger_table, fn x -> elem(x, 0) == next_hop_id end)
        send(elem(hd(next_hop_data), 1), {:find_key, target_key, pid, [id | source_ids]})
    end
  end

  def validate_with_holding_data(pid, id, loop_pid, holding_data, target_key) do
    cond do
      target_key in holding_data -> send(pid, {:found_id, {id, loop_pid}, :exists})
      true -> send(pid, {:found_id, {id, loop_pid}, :not_exists})
    end
  end

  def leave_chord(id, successor_data, predecessor_data, finger_updater_pid, pid, holding_data) do
    send(finger_updater_pid, {:stop})
    cond do
      successor_data != nil ->
        send(elem(successor_data, 1), {:recieve_own_data, holding_data})
        send(:global.whereis_name(@manager_name), {:leave, self(), id, successor_data, predecessor_data})
      true -> send(:global.whereis_name(@manager_name), {:leave, self(), id, successor_data, predecessor_data})
    end
    receive do
      {:exited} -> send(pid, {:ok})
    end
  end

  def update_predecessor_data(node_numbers, own_id, predecessor_id, predecessor_pid, holding_data) do
    own_valid_data = get_own_data(node_numbers, own_id, {predecessor_id, predecessor_pid})
    predecessor_holding_data = holding_data -- own_valid_data
    cond do
      length(predecessor_holding_data) != 0 ->
        send(predecessor_pid, {:recieve_own_data, predecessor_holding_data})
        holding_data -- predecessor_holding_data
      true -> holding_data
    end
  end

  def loop(finger_table, node_numbers, id, successor_data, predecessor_data, finger_updater_pid, holding_data) do
    receive do
      {:update_finger_table, new_finger_table} ->
        IO.puts("The new finger table is #{inspect finger_table}")
        loop(new_finger_table, node_numbers, id, successor_data, predecessor_data, finger_updater_pid, holding_data)
      {:new_successor, nil, nil} ->
        IO.puts("I'm all alone in the network now :( but it's cool that I don't need to update my finger table")
        send(finger_updater_pid, {:update_successor, nil})
        loop(finger_table, node_numbers, id, nil, predecessor_data, finger_updater_pid, holding_data)
      {:new_successor, successor_id, successor_pid} ->
        send(finger_updater_pid, {:update_successor, {successor_id, successor_pid}})
        IO.puts("New successor recognized with ID #{successor_id}}")
        loop(finger_table, node_numbers, id, {successor_id, successor_pid}, predecessor_data, finger_updater_pid, holding_data)
      {:new_predecessor, nil, nil} ->
        send(finger_updater_pid, {:update_predecessor, nil})
        loop(finger_table, node_numbers, id, successor_data, nil, finger_updater_pid, holding_data)
      {:new_predecessor, predecessor_id, predecessor_pid} ->
        send(finger_updater_pid, {:update_predecessor, {predecessor_id, predecessor_pid}})
        new_holding_data = update_predecessor_data(node_numbers, id, predecessor_id, predecessor_pid, holding_data)
        IO.puts("New predecessor recognized with ID #{predecessor_id}}")
        IO.puts("Currently holding #{inspect new_holding_data} as data")
        loop(finger_table, node_numbers, id, successor_data, {predecessor_id, predecessor_pid}, finger_updater_pid, new_holding_data)
      {:find_key, target_key, pid, source_ids} ->
        find_key(finger_table, target_key, pid, id, node_numbers, predecessor_data, self(), source_ids, holding_data)
        loop(finger_table, node_numbers, id, successor_data, predecessor_data, finger_updater_pid, holding_data)
      {:recieve_own_data, adding_holding_data} ->
        new_holding_data = Enum.sort(adding_holding_data ++ holding_data)
        IO.puts("Currently holding #{inspect new_holding_data} as data")
        loop(finger_table, node_numbers, id, successor_data, predecessor_data, finger_updater_pid, new_holding_data)
      {:leave_chord, pid} -> leave_chord(id, successor_data, predecessor_data, finger_updater_pid, pid, holding_data)
    end
  end

  def find(target_key, pid) do
    send(pid, {:find_key, target_key, self(), nil})
    receive do
      {:found_id, peer_data, :exists} -> IO.puts("Data with key #{target_key} found under ID #{elem(peer_data, 0)}")
      {:found_id, peer_data, :not_exists} -> IO.puts("Data with key #{target_key} does not exist under ID #{elem(peer_data, 0)}")
      {:not_found} -> IO.puts("Key not found. may be due to network instability")
      {:time_out} -> IO.puts("Connection timeout")
    end
  end

  def leave(pid) do
    send(pid, {:leave_chord, self()})
    receive do
      {:ok} -> IO.puts("Peer Exited Successfully")
    end
  end
end
