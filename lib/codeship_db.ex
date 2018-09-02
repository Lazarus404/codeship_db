defmodule CodeshipDB do
  use Application

  def start(_type, _args) do
    import Supervisor.Spec, warn: false

    children = [
      supervisor(CodeshipDB.Store.Super, [[]]),
      worker(CodeshipDB.Store.Manager, [])
    ]

    opts = [strategy: :one_for_one, name: CodeshipDB.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
