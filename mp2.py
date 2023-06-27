
import psycopg2
from seller import Seller
from messages import *
import configparser
import uuid
import datetime
import sys


def tokenize_command(command):
    tokens = command.split(" ")
    return [t.strip() for t in tokens]


class Mp2Client:
    def __init__(self, config_filename):
        config = configparser.ConfigParser()
        config.read('database.cfg')
        self.db_conn_params = config["postgresql"]
        self.conn = None

    """
        Connects to PostgreSQL database and returns connection object.
    """

    def connect(self):
        self.conn = psycopg2.connect(**self.db_conn_params)
        self.conn.autocommit = False

    """
        Disconnects from PostgreSQL database.
    """

    def disconnect(self):
        self.conn.close()

    """
        Prints list of available commands of the software.
    """

    def help(self):
        # prints the choices for commands and parameters
        print("\n*** Please enter one of the following commands ***")
        print("> help")
        print("> sign_up <seller_id> <subscriber_key> <zip_code> <city> <state> <plan_id>")
        print("> sign_in <seller_id> <subscriber_key>")
        print("> sign_out")
        print("> show_plans")
        print("> show_subscription")
        print("> change_stock <product_id> <add or remove> <amount>")
        print("> show_quota")
        print("> subscribe <plan_id>")
        print("> ship <product_id_1> <product_id_2> <product_id_3> ... <product_id_n>")
        print("> calc_gross")
        print("> show_cart <customer_id>")
        print(
            "> change_cart <customer_id> <product_id> <seller_id> <add or remove> <amount>")
        print("> purchase_cart <customer_id>")
        print("> quit")

    def sign_up(self, seller_id, sub_key, zip, city, state, plan_id):  # TESTED - FULL SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT seller_id FROM sellers WHERE seller_id = %s", (seller_id,))
            if cur.fetchone() is not None:
                return False, CMD_EXECUTION_FAILED

            cur.execute(
                "INSERT INTO sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state) VALUES (%s, %s, %s, %s)",
                (seller_id, zip, city, state)
            )

            cur.execute(
                "INSERT INTO seller_subscription (seller_id, subscriber_key, session_count, plan_id) VALUES (%s, %s, 0, %s)",
                (seller_id, sub_key, plan_id)
            )

            self.conn.commit()
            return True, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return False, CMD_EXECUTION_FAILED

    def sign_in(self, seller_id, sub_key):  # TESTED - FULL SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT * FROM sellers WHERE seller_id = %s", (seller_id,))
            seller = cur.fetchone()

            cur.execute(
                "SELECT * FROM seller_subscription WHERE seller_id = %s", (seller_id,))
            subscription = cur.fetchone()

            if seller is None or subscription[1] != sub_key:
                cur.close()
                return None, USER_SIGNIN_FAILED

            cur.execute("SELECT max_parallel_sessions FROM subscription_plans WHERE plan_id = %s",
                        (subscription[3],))
            max_parallel_sessions = cur.fetchone()[0]

            if subscription[2] >= max_parallel_sessions:
                cur.close()
                return None, USER_ALL_SESSIONS_ARE_USED

            cur.execute("UPDATE seller_subscription SET session_count = session_count + 1 "
                        "WHERE seller_id = %s",
                        (seller_id,))

            self.conn.commit()
            cur.close()

            return seller_id, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return None, DEBUG

        finally:
            if cur is not None:
                cur.close()

    def sign_out(self, seller):
        try:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT * FROM seller_subscription WHERE seller_id = %s", (seller,))
            result = cur.fetchone()

            if result is None:
                return False, CMD_EXECUTION_FAILED

            if result and result[2] > 0:
                cur.execute(
                    "UPDATE seller_subscription SET session_count = session_count - 1 WHERE seller_id = %s", (seller,))
                self.conn.commit()
                return True, CMD_EXECUTION_SUCCESS

            else:
                return False, CMD_EXECUTION_FAILED

        except Exception as e:
            self.conn.rollback()
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()

    def quit(self, seller):  # HELP!
        self.sign_out(seller)
        self.conn.close()
        sys.exit(0)

    def show_plans(self):  # TESTED - FULL SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute("SELECT * FROM subscription_plans")
            plans = cur.fetchall()

            cur.close()

            print("#|Name|Max Sessions|Max Stocks Per Product")
            for plan in plans:
                print(
                    f"{plan[0]}|{plan[1]}|{plan[2]}|{plan[3]}")

            return plans, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()

    def show_subscription(self, seller):  # TESTED - FULL SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT * FROM subscription_plans WHERE plan_id = (SELECT plan_id FROM seller_subscription WHERE seller_id = %s)", (seller[0]))
            subscription = cur.fetchone()

            cur.close()
            print(subscription)
            print("#|Name|Max Sessions|Max Stocks Per Product")
            print(
                f"{subscription[0]}|{subscription[1]}|{subscription[2]}|{subscription[3]}")

            return subscription, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return None, CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()

    def change_stock(self, seller, product_id, change_amount):  # TESTED - FULL SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute("SELECT stock_count FROM seller_stocks WHERE seller_id = %s AND product_id = %s",
                        (seller[0], product_id))
            stock = cur.fetchone()
            if stock is None:
                cur.close()
                return False, PRODUCT_NOT_FOUND

            stock_count = stock[0]
            new_stock_count = stock_count + change_amount

            cur.execute("SELECT max_stock_per_product FROM subscription_plans "
                        "JOIN seller_subscription ON subscription_plans.plan_id = seller_subscription.plan_id "
                        "WHERE seller_subscription.seller_id = %s", (seller[0],))
            max_stock_limit = cur.fetchone()[0]

            if new_stock_count < 0 or new_stock_count > max_stock_limit:
                cur.close()
                return False, STOCK_UPDATE_FAILURE

            cur.execute("UPDATE seller_stocks SET stock_count = %s WHERE seller_id = %s AND product_id = %s",
                        (new_stock_count, seller[0], product_id))

            self.conn.commit()
            cur.close()

            return True, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()

    def show_quota(self, seller):  # TESTED - FULL SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT product_id, stock_count FROM seller_stocks WHERE seller_id = %s", (seller[0],))
            quotas = cur.fetchall()

            cur.close()

            if not quotas:
                print("Quota limit is not activated yet.")
            else:
                print("Product Id|Remaining Quota")
                for quota in quotas:
                    print(f"{quota[0]}|{quota[1]}")

            return True, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()

    def subscribe(self, seller, plan_id):  # TESTED - SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT * FROM subscription_plans WHERE plan_id = %s", (plan_id,))
            new_plan = cur.fetchone()

            if new_plan is None:
                cur.close()
                return seller, SUBSCRIBE_PLAN_NOT_FOUND

            cur.execute(
                "SELECT max_parallel_sessions from subscription_plans where subscription_plans.plan_id = %s", (plan_id,))
            current_max_sessions = cur.fetchone()[0]

            if current_max_sessions > new_plan[2]:
                cur.close()
                return seller, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE

            cur.execute("UPDATE seller_subscription SET plan_id = %s WHERE seller_id = %s",
                        (plan_id, seller[0]))
            self.conn.commit()
            cur.close()
            return seller, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return seller, CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()

    def ship(self, seller, product_ids):  # TESTED - FULL SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT product_id FROM seller_stocks WHERE seller_id = %s", (seller[0],))
            existing_product_ids = set(row[0] for row in cur.fetchall())

            if not set(product_ids).issubset(existing_product_ids):
                cur.close()
                return False, CMD_EXECUTION_FAILED

            for product_id in product_ids:
                cur.execute("SELECT stock_count FROM seller_stocks WHERE seller_id = %s AND product_id = %s",
                            (seller[0], product_id))
                stock = cur.fetchone()

                if stock is None:
                    cur.close()
                    return False, CMD_EXECUTION_FAILED

                stock_count = stock[0]

                new_stock_count = stock_count - 1

                if new_stock_count < 0:
                    cur.close()
                    return False, CMD_EXECUTION_FAILED

                cur.execute("UPDATE seller_stocks SET stock_count = %s WHERE seller_id = %s AND product_id = %s",
                            (new_stock_count, seller[0], product_id))

            self.conn.commit()
            cur.close()

            return True, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()

    def calc_gross(self, seller):
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT COUNT(*) FROM orders WHERE customer_id = %s", (seller[0],))
            count = cur.fetchone()[0]

            if count == 0:
                cur.close()
                print("Gross Income: 0")
                return True, ""

            cur.execute("SELECT SUM(order_items.price) AS gross_income, "
                        "EXTRACT(YEAR FROM orders.order_purchase_timestamp) AS year, "
                        "EXTRACT(MONTH FROM orders.order_purchase_timestamp) AS month "
                        "FROM orders "
                        "JOIN order_items ON orders.order_id = order_items.order_id "
                        "WHERE orders.customer_id = %s "
                        "GROUP BY year, month "
                        "ORDER BY year, month", (seller[0],))

            results = cur.fetchall()

            print("Gross Income|Year|Month")
            for row in results:
                print(f"{row[0]:.2f}|{int(row[1])}|{int(row[2])}")

            cur.close()
            return True, ""

        except Exception as e:
            return False, CMD_EXECUTION_FAILED

    def show_cart(self, customer_id):  # TESTED - FULL SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT * FROM customers WHERE customer_id = %s", (customer_id,))
            customer = cur.fetchone()
            if customer is None:
                cur.close()
                return False, "ERROR: Customer is not found."

            cur.execute("SELECT seller_id, product_id, amount FROM customer_carts WHERE customer_id = %s",
                        (customer_id,))
            results = cur.fetchall()
            cur.close()

            output = "Seller Id|Product Id|Amount\n"
            for row in results:
                print(f"{row[0]}|{row[1]}|{row[2]}\n")

            return True, CMD_EXECUTION_SUCCESS

        except Exception as e:
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()

    def change_cart(self, customer_id, product_id, seller_id, change_amount):  # TESTED - FULL SUCCESS
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT * FROM customers WHERE customer_id = %s", (customer_id,))
            customer = cur.fetchone()

            if customer is None:
                cur.close()
                return False, CUSTOMER_NOT_FOUND

            cur.execute(
                "SELECT * FROM products WHERE product_id = %s", (product_id,))
            product = cur.fetchone()

            if product is None:
                cur.close()
                return False, PRODUCT_NOT_FOUND

            cur.execute("SELECT amount FROM customer_carts WHERE customer_id = %s AND product_id = %s AND seller_id = %s",
                        (customer_id, product_id, seller_id))
            cart_item = cur.fetchone()

            if cart_item is None:
                if change_amount > 0:
                    cur.execute("INSERT INTO customer_carts (customer_id, product_id, seller_id, amount) "
                                "VALUES (%s, %s, %s, %s)",
                                (customer_id, product_id, seller_id, change_amount))
                    self.conn.commit()
                    cur.close()
                    return True, CMD_EXECUTION_SUCCESS

            else:
                current_amount = cart_item[0]
                new_amount = current_amount + change_amount

                if new_amount <= 0:
                    cur.execute("DELETE FROM customer_carts WHERE customer_id = %s AND product_id = %s AND seller_id = %s",
                                (customer_id, product_id, seller_id))

                elif new_amount > current_amount:
                    cur.execute("SELECT stock_count FROM seller_stocks WHERE seller_id = %s AND product_id = %s",
                                (seller_id, product_id))
                    stock = cur.fetchone()

                    if stock is None or new_amount > stock[0]:
                        cur.close()
                        return False, STOCK_UNAVAILABLE

                cur.execute("UPDATE customer_carts SET amount = %s "
                            "WHERE customer_id = %s AND product_id = %s AND seller_id = %s",
                            (new_amount, customer_id, product_id, seller_id))

            self.conn.commit()
            cur.close()

            return True, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()

    def purchase_cart(self, customer_id):
        try:
            cur = self.conn.cursor()

            cur.execute(
                "SELECT * FROM customers WHERE customer_id = %s", (customer_id,))
            customer = cur.fetchone()

            if customer is None:
                cur.close()
                return False, CUSTOMER_NOT_FOUND

            cur.execute("SELECT seller_id, product_id, amount FROM customer_carts WHERE customer_id = %s",
                        (customer_id,))
            cart_items = cur.fetchall()

            for seller_id, product_id, amount in cart_items:
                cur.execute("SELECT stock_count FROM seller_stocks WHERE seller_id = %s AND product_id = %s",
                            (seller_id, product_id))
                stock = cur.fetchone()

                if stock is None or amount > stock[0]:
                    cur.close()
                    return False, STOCK_UNAVAILABLE

            order_id = uuid.uuid4().hex

            timestamp = datetime.datetime.now()

            cur.execute("INSERT INTO orders (order_id, customer_id, order_purchase_timestamp) "
                        "VALUES (%s, %s, %s)",
                        (order_id, customer_id, timestamp))

            order_item_id = 1
            for seller_id, product_id, amount in cart_items:
                cur.execute("INSERT INTO order_items (order_id, order_item_id, product_id, seller_id) "
                            "VALUES (%s, %s, %s, %s)",
                            (order_id, order_item_id, product_id, seller_id))
                order_item_id += 1

                cur.execute("UPDATE seller_stocks SET stock_count = stock_count - %s "
                            "WHERE seller_id = %s AND product_id = %s",
                            (amount, seller_id, product_id))

            cur.execute(
                "DELETE FROM customer_carts WHERE customer_id = %s", (customer_id,))

            self.conn.commit()
            cur.close()

            return True, CMD_EXECUTION_SUCCESS

        except Exception as e:
            self.conn.rollback()
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur is not None:
                cur.close()
