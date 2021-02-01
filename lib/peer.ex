defmodule Peer do
  @manager_name :chord_manager
  def start() do
    spawn(__MODULE__, :join, [])
  end


  def join() do
    send(:global.whereis_name(@manager_name), {:join, self()})
    receive do
      {:reject} -> exit(self())
      {:ok, node_numbers, id, successor_data, predecessor_data} -> run(node_numbers, id, successor_data, predecessor_data)
    end
  end


  def run(node_numbers, id, nil, nil) do
    finger_updater_pid = spawn(__MODULE__, :finger_update_manager, [self(), node_numbers, id, nil, nil])
    loop(%{}, node_numbers, id, nil, nil, finger_updater_pid)
  end
  def run(node_numbers, id, successor_data, predecessor_data) do
    finger_updater_pid = spawn(__MODULE__, :finger_update_manager, [self(), node_numbers, id, successor_data, predecessor_data])
    loop(%{elem(successor_data, 0) => elem(successor_data, 1)}, node_numbers, id, successor_data, predecessor_data, finger_updater_pid)
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



  def finger_update_manager(loop_pid, node_numbers, id, nil, predecessor_data) do
    receive do
      {:update_successor, new_successor_data} -> finger_update_manager(loop_pid, node_numbers, id, new_successor_data, predecessor_data)
      {:update_predecessor, new_predecessor_data} -> finger_update_manager(loop_pid, node_numbers, id, nil, new_predecessor_data)
    end
  end
  def finger_update_manager(loop_pid, node_numbers, id, successor_data, nil) do
    receive do
      {:update_successor, new_successor_data} -> finger_update_manager(loop_pid, node_numbers, id, new_successor_data, nil)
      {:update_predecessor, new_predecessor_data} -> finger_update_manager(loop_pid, node_numbers, id, successor_data, new_predecessor_data)
    end
  end
  def finger_update_manager(loop_pid, node_numbers, id, nil, nil) do
    receive do
      {:update_successor, new_successor_data} -> finger_update_manager(loop_pid, node_numbers, id, new_successor_data, nil)
      {:update_predecessor, new_predecessor_data} -> finger_update_manager(loop_pid, node_numbers, id, nil, new_predecessor_data)
    end
  end
  def finger_update_manager(loop_pid, node_numbers, id, successor_data, predecessor_data) do
    receive do
      {:update_successor, new_successor_data} -> finger_update_manager(loop_pid, node_numbers, id, new_successor_data, predecessor_data)
      {:update_predecessor, new_predecessor_data} -> finger_update_manager(loop_pid, node_numbers, id, successor_data, new_predecessor_data)
    after 5000 ->
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
        send(elem(successor_data, 1), {:find_key, new_id, self()})
        receive do
          {:found_id, peer_data} -> form_finger_table(Map.put(current_finger_table, elem(peer_data, 0), elem(peer_data, 1)), node_numbers, id, successor_data, predecessor_data, i + 1)
          {:not_found} -> form_finger_table(current_finger_table, node_numbers, id, successor_data, predecessor_data, i + 1)
          {:time_out} -> form_finger_table(current_finger_table, node_numbers, id, successor_data, predecessor_data, i + 1)
        end
    end

  end
  def form_finger_table(current_finger_table, _, _, _, _, _) do
    current_finger_table
  end

  def update_finger_table(loop_pid, node_numbers, id, successor_data, predecessor_data) do
    new_finger_table = Map.put(%{}, elem(successor_data, 0), elem(successor_data, 1))
    new_finger_table = form_finger_table(new_finger_table, node_numbers, id, successor_data, predecessor_data, 1)
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


  def find_key(finger_table, target_key, pid, id, node_numbers, predecessor_data, loop_pid) do
    peer_holding_data = get_own_data(node_numbers, id, predecessor_data)
    finger_keys_list = List.delete(Map.keys(finger_table), id)
    cond do
      target_key in peer_holding_data -> send(pid, {:found_id, {id, loop_pid}})
      length(finger_keys_list) == 0 -> send(pid, {:not_found})
      true ->
        next_hop_id = find_path_to_id(finger_keys_list, target_key, node_numbers)
        send(finger_table[next_hop_id], {:find_key, target_key, self()})
        receive do
          {:found_id, peer_data} -> send(pid, {:found_id, peer_data})
          {:not_found} -> send(pid, {:not_found})
        after 1000 ->
          send(pid, {:time_out})
        end
    end
  end


  def loop(finger_table, node_numbers, id, successor_data, predecessor_data, finger_updater_pid) do
    receive do
      {:update_finger_table, new_finger_table} ->
        IO.puts("New Finger Table is #{inspect finger_table}")
        loop(new_finger_table, node_numbers, id, successor_data, predecessor_data, finger_updater_pid)
      {:new_successor, successor_id, successor_pid} ->
        send(finger_updater_pid, {:update_successor, {successor_id, successor_pid}})
        loop(finger_table, node_numbers, id, {successor_id, successor_pid}, predecessor_data, finger_updater_pid)
      {:new_predecessor, predecessor_id, predecessor_pid} ->
        send(finger_updater_pid, {:update_predecessor, {predecessor_id, predecessor_pid}})
        loop(finger_table, node_numbers, id, successor_data, {predecessor_id, predecessor_pid}, finger_updater_pid)
      {:find_key, target_key, pid} ->
        spawn(__MODULE__, :find_key, [finger_table, target_key, pid, id, node_numbers, predecessor_data, self()])
        loop(finger_table, node_numbers, id, successor_data, predecessor_data, finger_updater_pid)
    end
  end


  def find(target_key, pid) do
    send(pid, {:find_key, target_key, self()})
    receive do
      {:found_id, peer_data} -> IO.puts("Key #{target_key} found under ID #{elem(peer_data, 0)}")
      {:not_found} -> IO.puts("Key not found. may be due to network unstability")
      {:time_out} -> IO.puts("Connection timeout")
    end
  end


end
