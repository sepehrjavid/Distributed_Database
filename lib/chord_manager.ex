defmodule ChordManager do
  @name :chord_manager

  def generate_data(available_data, 1) do
    number = Enum.random(available_data)
    {[number], List.delete(available_data, number)}
  end
  def generate_data(available_data, data_number) do
    {data, new_available_data} = generate_data(available_data, data_number - 1)
    number = Enum.random(new_available_data)
    {[number | data], List.delete(new_available_data, number)}
  end

  def start(node_numbers, data_number) do
    {data, _new_available_data}  = generate_data(Enum.to_list(0..node_numbers - 1), data_number)
    IO.puts("Network's data contain #{inspect Enum.sort(data)}")
    pid = spawn(__MODULE__, :loop, [%{}, node_numbers, Enum.sort(data)])
		:global.register_name(@name, pid)
  end

  def get_random_id(active_nodes, node_numbers) do
    assigned_ids = Map.keys(active_nodes)
    all_ids = Enum.to_list(0..node_numbers - 1)
    allowed_ids = Enum.filter(all_ids, fn x -> x not in assigned_ids end)
    Enum.random(allowed_ids)
  end

  def find_successor(head, [], _) do
    head
  end
  def find_successor(_, [possible_successor | _], peer_id) when possible_successor > peer_id do
    possible_successor
  end
  def find_successor(head, [_ | active_ids], peer_id) do
    find_successor(head, active_ids, peer_id)
  end

  def find_predecessor(head, [], _) do
    head
  end
  def find_predecessor(_, [possible_predecessor | _], peer_id) when possible_predecessor < peer_id do
    possible_predecessor
  end
  def find_predecessor(head, [_ | active_ids], peer_id) do
    find_predecessor(head, active_ids, peer_id)
  end


  def advertise_successor(successor_id, successor_pid, peer_pid) do
    send(peer_pid, {:new_successor, successor_id, successor_pid})
  end

  def advertise_predecessor(predecessor_id, predecessor_pid, peer_pid) do
    send(peer_pid, {:new_predecessor, predecessor_id, predecessor_pid})
  end


  def join(active_nodes, pid, node_numbers, holding_data) when map_size(active_nodes) == 0 do
    id = get_random_id(active_nodes, node_numbers)
    send(pid, {:ok, node_numbers, id, nil, nil, holding_data})
    Map.put(active_nodes, id, pid)
  end
  def join(active_nodes, pid, node_numbers, _) when map_size(active_nodes) == node_numbers do
    send(pid, {:reject})
    active_nodes
  end
  def join(active_nodes, pid, node_numbers, _) do
    id = get_random_id(active_nodes, node_numbers)
    active_ids = Map.keys(active_nodes)
    sorted_active_ids = Enum.sort(active_ids)
    reversed_sorted_active_ids = Enum.reverse(sorted_active_ids)
    successor_id = find_successor(hd(sorted_active_ids), sorted_active_ids, id)
    predecessor_id = find_predecessor(hd(reversed_sorted_active_ids), reversed_sorted_active_ids, id)
    new_active_nodes = Map.put(active_nodes, id, pid)
    advertise_successor(id, pid, active_nodes[predecessor_id])
    advertise_predecessor(id, pid, active_nodes[successor_id])
    send(pid, {:ok, node_numbers, id, {successor_id, active_nodes[successor_id]}, {predecessor_id, active_nodes[predecessor_id]}, nil})
    new_active_nodes
  end


  def leave(active_nodes, pid, id, nil, nil) do
    send(pid, {:exited})
    Map.delete(active_nodes, id)
  end
  def leave(active_nodes, pid, id, successor, predecessor) do
    new_active_nodes = Map.delete(active_nodes, id)
    cond do
      map_size(new_active_nodes) == 1 ->
        advertise_successor(nil, nil, elem(predecessor, 1))
        advertise_predecessor(nil, nil, elem(successor, 1))
      map_size(new_active_nodes) > 1 ->
        advertise_successor(elem(successor, 0), elem(successor, 1), elem(predecessor, 1))
        advertise_predecessor(elem(predecessor, 0), elem(predecessor, 1), elem(successor, 1))
    end
    send(pid, {:exited})
    new_active_nodes
  end

  def loop(active_nodes, node_numbers, holding_data) do
    IO.puts("Active Nodes are #{inspect active_nodes}")
    receive do
      {:join, pid} ->
        new_active_nodes = join(active_nodes, pid, node_numbers, holding_data)
        loop(new_active_nodes, node_numbers, holding_data)
      {:leave, pid, id, successor, predecessor} ->
        new_active_nodes = leave(active_nodes, pid, id, successor, predecessor)
        loop(new_active_nodes, node_numbers, holding_data)
    end
  end
end
