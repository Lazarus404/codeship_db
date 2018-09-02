defmodule CodeshipDB.Pkt.Params do
  def attrs(),
    do: [
      {:bucket, :value},
      {:json, :value},
      {:data, :value},
      {:username, :value},
      {:password, :value},
      {:realm, :value},
      {:message_integrity, :value},
      {:error_code, :error_attribute},
      {:padding, :value}
    ]

  def methods(),
    do: [
      :bucket_exists,
      :destroy,
      :get,
      :set,
      :update,
      :del,
      :del_all
    ]

  def classes(),
    do: [
      :error,
      :request,
      :response,
      :ack
    ]
end
