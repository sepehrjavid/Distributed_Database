defmodule ChordManager do
  @name :chord_manager

  def start(node_numbers) do
    pid = spawn(__MODULE__, :loop, [%{}, node_numbers])
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


  def join(active_nodes, pid, node_numbers) when map_size(active_nodes) == 0 do
    id = get_random_id(active_nodes, node_numbers)
    send(pid, {:ok, node_numbers, id, nil, nil})
    Map.put(active_nodes, id, pid)
  end
  def join(active_nodes, pid, node_numbers) when map_size(active_nodes) == node_numbers do
    send(pid, {:reject})
    active_nodes
  end
  def join(active_nodes, pid, node_numbers) do
    id = get_random_id(active_nodes, node_numbers)
    active_ids = Map.keys(active_nodes)
    sorted_active_ids = Enum.sort(active_ids)
    reversed_sorted_active_ids = Enum.reverse(sorted_active_ids)
    successor_id = find_successor(hd(sorted_active_ids), sorted_active_ids, id)
    predecessor_id = find_predecessor(hd(reversed_sorted_active_ids), reversed_sorted_active_ids, id)
    new_active_nodes = Map.put(active_nodes, id, pid)
    send(pid, {:ok, node_numbers, id, {successor_id, active_nodes[successor_id]}, {predecessor_id, active_nodes[predecessor_id]}})
    advertise_successor(id, pid, active_nodes[predecessor_id])
    advertise_predecessor(id, pid, active_nodes[successor_id])
    new_active_nodes
  end


  def leave(_active_nodes, _pid, _node_numbers) do

  end

  def loop(active_nodes, node_numbers) do
    receive do
      {:join, pid} ->
        new_active_nodes = join(active_nodes, pid, node_numbers)
        IO.puts(inspect new_active_nodes)
        loop(new_active_nodes, node_numbers)
      {:leave, pid} ->
        new_active_nodes = leave(active_nodes, pid, node_numbers)
        loop(new_active_nodes, node_numbers)
    end
  end
end
