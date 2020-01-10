defmodule Gosspush do
  def main(args \\ []) do

    {_, inputParam, _}  = OptionParser.parse(args, switches: [])
    numOfNodes  = String.to_integer(Enum.at(inputParam, 0))
    topology    = Enum.at(inputParam, 1)
    algorithm   = Enum.at(inputParam, 2)
    initialTime = System.monotonic_time(:millisecond)

    case algorithm do
      "gossip"      ->
          actors      = getActorsForGossip(numOfNodes)
          neighbors   = getNeighbours(actors,topology)
          IO.puts("Gossip Algorithm Initiated. Gossiping...")
          runGossipAlgorithm(actors, neighbors, numOfNodes)
      "pushsum"     ->
          actors      = getActorsForPushSum(numOfNodes)
          neighbors   = getNeighbours(actors,topology)
          IO.puts("Push-sum Algorithm Initiated. Calculating...")
          runPushSumAlgorithm(actors, neighbors, numOfNodes)
      _anyAlgorithm ->
          IO.puts("Invalid Algorithm! Choose Gossip or PushSum.")
          System.halt(0)
    end

    IO.puts "Converging Time : " <> to_string(System.monotonic_time(:millisecond) - initialTime) <> " ms"
    System.halt(0)

  end

  def getActorsForGossip(numOfNodes) do
    gossipStartNode = trunc(numOfNodes/2)
    Enum.map(1..numOfNodes, fn z -> {:ok, actor} =
      cond do
        z == gossipStartNode -> GossGenServ.start_link("Begin rumour spreading process")
        true -> GossGenServ.start_link("")
      end
      actor
    end)
  end

  def getActorsForPushSum(numOfNodes) do
    pushSumStartNode = trunc(numOfNodes/2)
    Enum.map(1..numOfNodes, fn z-> {:ok, actor} =
      cond do
        z == pushSumStartNode ->
          z       = Integer.to_string(z)
          {z, _}  = Float.parse(z)
          GossGenServ.start_link([z] ++ ["Begin rumour spreading process"])
        true ->
          z       = Integer.to_string(z)
          {z, _}  = Float.parse(z)
          GossGenServ.start_link([z] ++ [""])
      end
      actor
    end)
  end

  def getNeighbours(actors,topology) do
    neighbors =
    case topology do
      "full"          ->
          IO.puts("Getting neighbors of Full Topology...")
          _neighbors = getFullTopologyNeighbors(actors)
      "line"          ->
          IO.puts("Getting neighbors of Line Topology...")
          _neighbors = getLineTopologyNeighbors(actors)
      "random2d"      ->
          IO.puts("Getting neighbors of Random 2D Topology...")
          _neighbors = getRandom2DTopologyNeighbors(actors)
      "torus3d"       ->
          IO.puts("Getting neighbors of Torus 3D Topology...")
          _neighbors = getTorus3DTopologyNeighbors(actors)
      "honeycomb"     ->
          IO.puts("Getting neighbors of Honeycomb Topology...")
          _neighbors = getHoneycombTopologyNeighbors(actors, false)
      "randhoneycomb" ->
          IO.puts("Getting neighbors of Random Honeycomb Topology...")
          _neighbors = getHoneycombTopologyNeighbors(actors, true)
      _anyTopology    ->
          IO.puts "Invalid Topology. Terminating..."
          System.halt(0)
    end
    :ets.new(:reachVal, [:set, :public, :named_table])
    :ets.insert(:reachVal, {"messageReach", 0})
    setNeighborState(neighbors)
    neighbors
  end

  def getFullTopologyNeighbors(actors) do
      Enum.reduce(actors, %{}, fn (x, acc) ->
        Map.put(acc, x, Enum.filter(actors, fn y ->
          y != x end)
        )
      end)
  end

  def getLineTopologyNeighbors(actors) do
    actorPointer  = Stream.with_index(actors,0) |> Enum.reduce(%{}, fn ({z,indexKeyValue}, acc) ->
      Map.put(acc, indexKeyValue, z) end)
    actorLength   = length(actors)
    Enum.reduce(0..actorLength-1, %{}, fn(z, acc) ->
      neighbors =
        cond do
          z == 0 -> [z+1]
          z == actorLength-1 -> [actorLength-2]
          true -> [(z-1), (z+1)]
        end
        neighborPid = Enum.map(neighbors, fn j ->
          {:ok, actorLength} = Map.fetch(actorPointer, j)
        actorLength end)
        {:ok, actor} = Map.fetch(actorPointer, z)
        Map.put(acc, actor, neighborPid)
    end)
  end

  def getRandom2DTopologyNeighbors(actors) do
    actorLength   = length(actors)
    tempVar       = %{}
    actorPointer  = Stream.with_index(actors,0) |> Enum.reduce(%{}, fn ({z,indexKeyValue}, acc) ->
      Map.put(acc, indexKeyValue, z) end)
    actorsRandPos = Enum.map(0..actorLength-1, fn z-> Map.put(tempVar, z, [:rand.uniform(100)] ++ [:rand.uniform(100)]) end)
    Enum.reduce(actorsRandPos, %{}, fn i,acc ->
      currNodeVal = Enum.at(Map.values(i),0)
      currNodeKey = Enum.at(Map.keys(i),0)
      compareNeighborList = List.delete(actorsRandPos,i)
      neighborList = [] ++ Enum.map(compareNeighborList, fn j ->
        if check2DRandDist(currNodeVal, Enum.at(Map.values(j),0)) do
          Enum.at(Map.keys(j),0)
        end
      end)
      neighborList = Enum.filter(neighborList, &(!is_nil(&1)))
      if Enum.any?(neighborList) do
        neighborPid = Enum.map(neighborList, fn z ->
          {:ok, actorLength} = Map.fetch(actorPointer, z)
        actorLength end)
        {:ok, actor} = Map.fetch(actorPointer, currNodeKey)
        Map.put(acc, actor, neighborPid)
      else
        acc
      end
    end)
  end

  def getTorus3DTopologyNeighbors(actors) do
    actorLength   = length(actors)
    keyRowCount   = trunc(ceil(:math.pow(actorLength,1/3)))
    actorPointer  = Stream.with_index(actors,0) |> Enum.reduce(%{}, fn ({z,indexKeyValue}, acc) ->
      Map.put(acc, indexKeyValue, z) end)
    Enum.reduce(1..actorLength, %{}, fn (i, acc) ->
      level       = trunc(ceil(i / (keyRowCount * keyRowCount)))
      upperLimit  = level * keyRowCount * keyRowCount
      lowerLimit  = ((level-1) * keyRowCount * keyRowCount) + 1
      neighborsList =
        Enum.reduce(1..9, %{}, fn (j, acc) ->
          if ((i - keyRowCount) >= lowerLimit) && (j == 1) do
            Map.put(acc, j, (i - keyRowCount - 1))
          else
            if ((i + keyRowCount) <= upperLimit) && (j == 2) do
              Map.put(acc, j, (i + keyRowCount - 1))
            else
              if (rem(i, keyRowCount) != 1) && ((i - 1) > 0) && (j == 3) do
                Map.put(acc, j, (i - 2))
              else
                if (rem(i, keyRowCount) != 0) && ((i+1) <= actorLength) && (j == 4) do
                  Map.put(acc, j, i)
                else
                  if (i + (keyRowCount * keyRowCount) <= actorLength) && (j == 5) do
                    Map.put(acc, j, (i + (keyRowCount * keyRowCount) - 1))
                  else
                    if (i - (keyRowCount * keyRowCount) > 0) && (j == 6) do
                      Map.put(acc, j, (i - (keyRowCount * keyRowCount) - 1))
                    else
                      if (level == 1 or level == keyRowCount) && (j == 7) do
                        oppositeFace1 = (keyRowCount - 1) * (keyRowCount * keyRowCount)
                        if level == 1 do
                          Map.put(acc, j, (i + oppositeFace1 - 1))
                        else
                          Map.put(acc, j, (i - oppositeFace1 - 1))
                        end
                      else
                        if ((rem(i , keyRowCount) == 0) or (rem(i, keyRowCount) == 1)) && (j == 8) do
                          oppositeFace2 = (keyRowCount - 1)
                          if (rem(i , keyRowCount) == 1) do
                            Map.put(acc, j, (i + oppositeFace2 - 1))
                          else
                            Map.put(acc, j, (i - oppositeFace2 - 1))
                          end
                        else
                          if (((i + ((keyRowCount - 1) * keyRowCount)) <= upperLimit) or ((i - ((keyRowCount - 1) * keyRowCount)) >= lowerLimit)) && (j == 9) do
                            if  ((i + ((keyRowCount - 1) * keyRowCount)) <= upperLimit) do
                              Map.put(acc, j, (i + ((keyRowCount - 1) * keyRowCount) - 1))
                            else
                              Map.put(acc, j, (i - ((keyRowCount - 1) * keyRowCount) - 1))
                            end
                          else
                            acc
                          end
                        end
                      end
                    end
                  end
                end
              end
            end
          end
        end)
      neighborsList = Map.values(neighborsList)
      neighborPid   = Enum.map(neighborsList, fn x ->
        {:ok, actorLength} = Map.fetch(actorPointer, x)
        actorLength
      end)
      {:ok, actor} = Map.fetch(actorPointer, i-1)
      Map.put(acc, actor, neighborPid)
    end)
  end

  def getHoneycombTopologyNeighbors(actors, isRandHoneycomb) do
    actorLength = length(actors)
    colCountVal = 6
    tempAddVal = if rem(actorLength,colCountVal) == 0 do colCountVal else rem(actorLength,colCountVal) end
    actorPointer  = Stream.with_index(actors,0) |> Enum.reduce(%{}, fn ({z,indexKeyValue}, acc) ->
      Map.put(acc, indexKeyValue, z) end)
    Enum.reduce(1..actorLength, %{}, fn i,acc ->
      oddRow  = rem(ceil(i/colCountVal),2) == 1
      evenRow = rem(ceil(i/colCountVal),2) == 0
      oddCol  = rem(i,2) == 1
      evenCol = rem(i,2) == 0
      neighborsList =
      cond do
        i == 1 or i == colCountVal -> [i+colCountVal-1]
        (i == actorLength-tempAddVal+1 or (i == actorLength and rem(actorLength,colCountVal) == 0)) and oddRow -> [i-colCountVal-1]
        i < colCountVal and evenCol -> [i, i+colCountVal-1]
        i < colCountVal and oddCol -> [i-2, i+colCountVal-1]
        i > actorLength-tempAddVal and evenCol and oddRow -> [i-colCountVal-1, i]
        i > actorLength-tempAddVal and oddCol and oddRow -> [i-colCountVal-1, i-2]
        i > actorLength-tempAddVal and evenCol and evenRow -> [i-colCountVal-1, i-2]
        i > actorLength-tempAddVal and oddCol and evenRow == 0 -> [i-colCountVal-1, i]
        oddRow and (rem(i,colCountVal) == 1 or rem(i,colCountVal) == 0) -> [i-colCountVal-1, i+colCountVal-1]
        evenRow and rem(rem(i,colCountVal),2) == 1 -> [i-colCountVal-1, i, i+colCountVal-1]
        oddRow and rem(rem(i,colCountVal),2) == 0 -> [i-colCountVal-1, i, i+colCountVal-1]
        evenRow and rem(rem(i,colCountVal),2) == 0 -> [i-colCountVal-1, i-2, i+colCountVal-1]
        oddRow and rem(rem(i,colCountVal),2) == 1 -> [i-colCountVal-1, i-2, i+colCountVal-1]
      end
      neighborsList = Enum.filter(neighborsList, fn j -> j<=actorLength-1 end)
      neighborsList =
      if isRandHoneycomb do
        randomNeighborList   = Enum.map(0..actorLength-1, fn z-> z  end) -- [i-1]
        randomNeighborList   = randomNeighborList -- neighborsList
        neighborsList ++ [Enum.random(randomNeighborList)]
      else
        neighborsList
      end
      neighborPid = Enum.map(neighborsList, fn z ->
        {:ok, actorLength} = Map.fetch(actorPointer, z)
        actorLength end)
      {:ok, actor} = Map.fetch(actorPointer, i-1)
      Map.put(acc, actor, neighborPid)
    end)
  end

  def setNeighborState(neighbors) do
    for{indexKeyValue, z} <- neighbors do
      GossGenServ.setNeighborState(indexKeyValue, z)
    end
  end

  def runGossipAlgorithm(actors, neighbors, numOfNodes) do
    for{indexKeyValue, _} <- neighbors do
      GossGenServ.sendGossipRumorMessage(indexKeyValue)
    end
    actors = isGossipActorAlive(actors)
    [{_, messageReach}] = :ets.lookup(:reachVal, "messageReach")
    if((messageReach != numOfNodes) && (length(actors) > 1)) do
      neighbors = Enum.filter(neighbors, fn {indexKeyValue,_} -> Enum.member?(actors, indexKeyValue) end)
      runGossipAlgorithm(actors, neighbors, numOfNodes)
    end
  end

  def runPushSumAlgorithm(actors, neighbors, numOfNodes) do
    for{indexKeyValue, _} <- neighbors do
      GossGenServ.sendPushSumMsgVal(indexKeyValue)
    end
    actors = isPushSumActorAlive(actors)
    [{_, messageReach}] = :ets.lookup(:reachVal, "messageReach")
    if((messageReach != numOfNodes) && (length(actors) > 1)) do
      neighbors = Enum.filter(neighbors, fn {indexKeyValue,_} -> Enum.member?(actors, indexKeyValue) end)
      runPushSumAlgorithm(actors, neighbors, numOfNodes)
    end
  end

  def isGossipActorAlive(actors) do
    aliveActors = Enum.map(actors, fn z ->
      if(Process.alive?(z) && GossGenServ.getCountVal(z) < 10 && GossGenServ.getNeighborList(z)) do
        z
      end
    end)
    List.delete(Enum.uniq(aliveActors), nil)
  end

  def isPushSumActorAlive(actors) do
    aliveActors = Enum.map(actors, fn z ->
      swRatioDiff = GossGenServ.getSWRatioDiff(z)
      if(Process.alive?(z) && GossGenServ.getNeighborList(z) &&
          (abs(List.first(swRatioDiff)) > :math.pow(10, -10) || abs(List.last(swRatioDiff)) > :math.pow(10, -10))) do
        z
      end
    end)
    List.delete(Enum.uniq(aliveActors), nil)
  end

  def check2DRandDist(node1, node2) do
    xCoOrdDiff  = :math.pow(Enum.at(node2, 0) - Enum.at(node1, 0), 2)
    yCoOrdDiff  = :math.pow(Enum.at(node2, 1) - Enum.at(node1, 1), 2)
    nodeDist    = round(:math.sqrt(xCoOrdDiff + yCoOrdDiff))
    cond do
      nodeDist <= 10 -> true
      nodeDist > 10 -> false
    end
  end

end
