defmodule GossGenServ do
  use GenServer

  def start_link(val) do
    GenServer.start_link(__MODULE__, val)
  end

  def setNeighborState(indexServer, neighbors) do
    GenServer.cast(indexServer, {:setNeighborState, neighbors})
  end

  def sendGossipRumorMessage(indexServer) do
    GenServer.cast(indexServer, {:sendGossipRumorMessage})
  end

  def sendPushSumMsgVal(indexServer) do
    GenServer.cast(indexServer, {:sendPushSumMsgVal})
  end

  def getCountVal(indexServer) do
    {:ok, countVal} = GenServer.call(indexServer, {:getCountVal, "countVal"})
    countVal
  end

  def getNeighborList(indexServer) do
    {:ok, neighborList} = GenServer.call(indexServer, {:getNeighborList})
    length(neighborList) > 0
  end

  def getSWRatioDiff(indexServer) do
    GenServer.call(indexServer, {:getSWRatioDiff})
  end

  def init(val) do
    if is_list(val) do
      {:ok, %{"sCurrVal" => List.first(val), "rumourVal" => List.last(val), "wCurrVal" => 1, "sInitialVal" => 1, "wInitialVal" => 1, "swRatioDiff1" => 1, "swRatioDiff2" => 1, "neighborList" => []}}
    else
      {:ok, %{"rumourVal" => val, "countVal" => 0, "neighborList" => []}}
    end
  end

  def handle_call({:getCountVal, countVal}, _from, state) do
    {:reply, Map.fetch(state, countVal), state}
  end

  def handle_call({:getNeighborList}, _from, state) do
    {:reply, Map.fetch(state, "neighborList"), state}
  end

  def handle_call({:getSWRatioDiff}, _from, state) do
    {:ok, swRatioDiff1} = Map.fetch(state, "swRatioDiff1")
    {:ok, swRatioDiff2} = Map.fetch(state, "swRatioDiff2")
    {:reply, [swRatioDiff1] ++ [swRatioDiff2], state}
  end

  def handle_cast({:setNeighborState, neighborList}, state) do
    {:noreply, Map.put(state, "neighborList", neighborList)}
  end

  def handle_cast({:sendGossipRumorMessage}, state) do
    {:ok, rumourVal}    = Map.fetch(state, "rumourVal")
    {:ok, neighborList} = Map.fetch(state, "neighborList")
    if(rumourVal != "" && length(neighborList) > 0) do
        _sendNeighborMsg = GenServer.cast(Enum.random(neighborList), {:receiveGossipRumorMessage, rumourVal, self()})
    end
    {:noreply, state}
  end

  def handle_cast({:sendPushSumMsgVal}, state) do
    {:ok, sCurrVal}     = Map.fetch(state, "sCurrVal")
    {:ok, wCurrVal}     = Map.fetch(state, "wCurrVal")
    {:ok, rumourVal}    = Map.fetch(state, "rumourVal")
    {:ok, neighborList} = Map.fetch(state, "neighborList")
    if(rumourVal != "" && length(neighborList) > 0) do
      sCurrVal = sCurrVal/2
      wCurrVal = wCurrVal/2
      state = Map.put(state, "sCurrVal", sCurrVal)
      state = Map.put(state, "wCurrVal", wCurrVal)
      GenServer.cast(Enum.random(neighborList), {:receivePushSumMsgVal, self(), sCurrVal, wCurrVal, rumourVal})
    end
    {:noreply, state}
  end

  def handle_cast({:receiveGossipRumorMessage, rumourVal, sender}, state) do
    {:ok, countVal} = Map.fetch(state, "countVal")
    state = Map.put(state, "countVal", countVal + 1)
    if(countVal > 10) do
       _deleteNeighborNode = GenServer.cast(sender, {:deleteNeighborNode, self()})
       {:noreply, state}
    else
        {:ok, currentRumorVal} = Map.fetch(state, "rumourVal")
        if(currentRumorVal != "") do
            {:noreply, state}
        else
            [{_, messageReach}] = :ets.lookup(:reachVal, "messageReach")
            :ets.insert(:reachVal, {"messageReach", messageReach + 1})
            {:noreply, Map.put(state, "rumourVal", rumourVal)}
        end
    end
  end

  def handle_cast({:receivePushSumMsgVal, sender, sIncomingVal, wIncomingVal, rumourVal}, state) do
    {:ok, sCurrVal}       = Map.fetch(state, "sCurrVal")
    {:ok, wCurrVal}       = Map.fetch(state, "wCurrVal")
    {:ok, sInitialVal}    = Map.fetch(state, "sInitialVal")
    {:ok, wInitialVal}    = Map.fetch(state, "wInitialVal")
    {:ok, currRumourVal}  = Map.fetch(state, "rumourVal")

    sNewVal = sCurrVal + sIncomingVal
    wNewVal = wCurrVal + wIncomingVal
    if(abs(sNewVal/wNewVal - sCurrVal/wCurrVal) < :math.pow(10,-10) && abs(sCurrVal/wCurrVal - sInitialVal/wInitialVal) < :math.pow(10,-10)) do
      GenServer.cast(sender, {:deleteNeighborNode, self()})
    else
      if(currRumourVal == "") do
        Map.put(state, "rumourVal", rumourVal)
        [{_, messageReach}] = :ets.lookup(:reachVal, "messageReach")
        :ets.insert(:reachVal, {"messageReach", messageReach + 1})
        Map.put(state, "sCurrVal", sNewVal)
        Map.put(state, "wCurrVal", wNewVal)
        Map.put(state, "sInitialVal", sCurrVal)
        Map.put(state, "wInitialVal", wCurrVal)
        Map.put(state, "swRatioDiff1", sNewVal/wNewVal - sCurrVal/wCurrVal)
        Map.put(state, "swRatioDiff2", sCurrVal/wCurrVal - sInitialVal/wInitialVal)
        {:noreply, state}
      else
        Map.put(state, "sCurrVal", sNewVal)
        Map.put(state, "wCurrVal", wNewVal)
        Map.put(state, "sInitialVal", sCurrVal)
        Map.put(state, "wInitialVal", wCurrVal)
        Map.put(state, "swRatioDiff1", sNewVal/wNewVal - sCurrVal/wCurrVal)
        Map.put(state, "swRatioDiff2", sCurrVal/wCurrVal - sInitialVal/wInitialVal)
        {:noreply, state}
      end
    end
  end

  def handle_cast({:deleteNeighborNode, neighbor}, state) do
    {:ok, neighborList} = Map.fetch(state, "neighborList")
    {:noreply, Map.put(state, "neighborList", List.delete(neighborList, neighbor))}
  end

end
