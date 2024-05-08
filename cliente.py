import Pyro5.api

def main():
    try:
        ns = Pyro5.api.locate_ns()
        print("Available objects:", ns.list())

        while True:
            value = input('Insert value (or "exit" to quit): ')
            if value.lower() == "exit":
                print("Exiting...")
                break

            leader_uri = ns.lookup('leader')
            print(f"Leader URI: {leader_uri}")
            leader_proxy = Pyro5.api.Proxy(leader_uri)
            print(f"Sending: {value}")
            leader_proxy.set_leader_value(value)

    except Pyro5.errors.NamingError as ne:
        print("Error locating the Name Server:", ne)
    except Pyro5.errors.CommunicationError as ce:
        print("Communication error:", ce)
    except KeyboardInterrupt:
        print("\nExiting...")

if __name__ == "__main__":
    main()
