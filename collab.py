# The Collab system consisting of the frontend and the backend

# . : NOTE : .
# All comments starting with '##' are used for debugging purpose
# Disable the comments to see whether the modules are firing or not

import sys        # For argument list
import xmlrpclib  # For XMLRPC
import os         # For making directories
import threading  # For running the server in the background
import time       # For calling the sleep function
import hashlib    # For hashing node addresses & file names
import Queue
import thread
import datetime

import config     # For using constants

from SimpleXMLRPCServer import SimpleXMLRPCServer

class collab_system:

	def __init__(self, local_ip, local_port):
		# IP of the current machine
		self.local_ip       = local_ip

		# Port number at which the process is running
		self.local_port     = local_port

		# Amount of data uploaded by the current node (in bytes)
		self.upload_amt     = 1

		# Amounf of data downloaded by the current node (in bytes)
		self.download_amt   = 1

		# The ratio of the upload data to the downloaded data
		self.ratio          = 1

		# Path of the files which are hosted by the current node
		self.dir_hosted     = "collab_hosted" + "_" + local_ip + "_" + local_port

		# Path of the files which have been downloaded by the current node
		self.dir_downloaded = "collab_downloaded" + "_" + local_ip + "_" + local_port

		# A data structure which contains a list of file names
		# which are hosted by the current node and their hashed equiv
		self.file_dict      = {}

		# Node ID which is a hash of the IP and port
		self.node_id		= self.mod_hash_string(local_ip + ":" + local_port)

		#---sourav---
		self.cache={}
		self.q=Queue.Queue()
		self.lock=thread.allocate_lock()
		#---sourav---


	# These functions are used in the frontend

	def return_pause(self):
		"""Used for creating a pause during input"""

		raw_input("\n\tPress enter to continue")

		return True

	def mod_file_download(self, file_name, remote_proxy):
		"""Sending details to remote node which will send file to local node"""

		hash_digest = self.mod_hash_string(file_name)

		download_file_size = remote_proxy.mod_file_download_transfer(hash_digest, self.local_port, self.ratio)

		if download_file_size == -1:
			return False
		else:
			self.download_amt = self.download_amt + download_file_size

			self.mod_update_ratio()

			return True

	def mod_file_upload(self, file_path, file_name, remote_proxy):
		"""Used for sending files to a receiver"""

		with open(file_path, "rb") as handle:
			bin_data = xmlrpclib.Binary(handle.read())

		if remote_proxy.mod_file_upload_receive(bin_data, file_name) == True:

			self.upload_amt = self.upload_amt + os.stat(file_path).st_size

			self.mod_update_ratio()

			return True

		else:
			return False

	def mod_show_stats(self):
		"""Shows all statistics of the current node"""

		print "\n\t\tUpload   (Bytes) : %d" % (self.upload_amt)
		print "\n\t\tDownload (Bytes) : %d" % (self.download_amt)
		print "\n\t\tCurrent ratio    : %f" % (self.ratio)

	def mod_show_files(self):
		"""Returns a list of files present in the current directory"""

		return self.file_dict.values()

	def mod_update_ratio(self):
		"""Updated the upload to download ratio"""

		self.ratio = (self.upload_amt * 1.0)/(self.download_amt * 1.0)

	# These functions are used in the backend

	def mod_file_upload_receive(self, bin_data, file_name):
		"""Used to receive a file upon a request of an upload"""

		##print "[mod_file_receive fired]"

		# File not present in node, thus upload can proceed
		if self.mod_hash_check_file(self.mod_hash_string(file_name)) == False:

			new_file_name = self.dir_hosted + "/" + file_name

			with open(new_file_name, "wb") as handle:
				handle.write(bin_data.data)

			# Adding the file name to the hashed list of files
			self.mod_file_dict_append(file_name)		

			return True
		else:

			return False

	def mod_file_download_transfer(self, given_hash, remote_port, remote_ratio):
		"""Initiating the file transfer"""

		##print "[mod_file_transfer fired]"

		self.mod_download_sleep(remote_ratio)

		# Checking if the file name hash actually exists
		file_name = self.mod_hash_check_file(given_hash)

		if file_name != False:

			# Creating path of local file about to be transferred
			file_path = self.dir_hosted + "/" + file_name

			with open(file_path, "rb") as handle:
				bin_data = xmlrpclib.Binary(handle.read())

			# Creating connection object of requestor
			remote_proxy = xmlrpclib.ServerProxy("http://localhost:" + remote_port + "/")
			
			# Connecting to requestor's server
			remote_proxy.mod_file_download_receive(bin_data, file_name)

			sent_file_size = os.stat(file_path).st_size

			self.upload_amt = self.upload_amt + sent_file_size

			self.mod_update_ratio()

			return sent_file_size
		else:
			return -1

	def mod_file_download_receive(self, bin_data, file_name):
		"""Used to receive a file upon request of a download"""

		##print "[mod_file_download_receive fired]"

		new_file_name = self.dir_downloaded + "/" + file_name

		with open(new_file_name, "wb") as handle:
			handle.write(bin_data.data)

		return True

	def mod_download_sleep(self, ratio):
		"""A function which delays the download according to the upload/download ratio"""

		sleep_time = 0

		# Ratio greater than 1
		if ratio > 1:
			sleep_time = 0

		# No download or upload done yet or ratio is exactly 1
		elif ratio == 1:
			sleep_time = config.SLEEP_TIME_LEVEL_DEF()

		# Ratio is less than 1
		elif ratio >= 0.1 and ratio < 1:
			sleep_time = config.SLEEP_TIME_LEVEL_1()

		elif ratio >= 0.01 and ratio < 0.1:
			sleep_time = config.SLEEP_TIME_LEVEL_2()

		elif ratio >= 0.001 and ratio < 0.01:
			sleep_time = config.SLEEP_TIME_LEVEL_3()

		else:
			sleep_time = config.SLEEP_TIME_LEVEL_4()

		##print "Sleep Time : %d" % self.mod_calc_sleep_time(ratio)
		time.sleep(sleep_time)

	# Hashing related functions

	def mod_file_dict_append(self, file_name):
		"""Appending file name and hashed file name to file dictionary"""

		hash_digest = self.mod_hash_string(file_name)

		self.file_dict[hash_digest] = file_name

	def mod_hash_string(self, given_str):
		"""A hashing function which hashes according to a predefined key space"""

		hash_digest = hashlib.sha1()
		hash_digest.update(given_str)

		return int(hash_digest.hexdigest(),16) % config.KEY_SPACE()

	def mod_hash_check_file(self, given_hash):
		"""A function which checks if a file is present in the hash list"""

		if self.file_dict.has_key(given_hash) == True:
			return self.file_dict[given_hash]

		else:
			return False





	#---------------Sourav & Midhun--------------------------------------------------


	# Cache to store previous downloads in each node
	def mod_cache_update(self,file_hash, actual_ip, actual_port):
		if len(self.cache) ==50:
			del_item=self.q.get()
			# print "Deleted item is :  ", del_item
			self.lock.acquire()
			del self.cache[del_item]
			self.lock.release()
		self.lock.acquire()
		showtime = datetime.datetime.now().strftime("%d-%m-%Y:%H-%M-%S")
		cache_list=[actual_ip, actual_port, showtime]
		self.cache[file_hash]=cache_list
		self.q.put(file_hash)
		self.lock.release()


    def mod_check_cache_validity(self,file_hash):
        if self.cache.has_key(file_hash):
            ttl_live= format((datetime.datetime.strptime(dict.get(file_hash)[1],"%d-%m-%Y:%H-%M-%S" )+ datetime.timedelta(seconds=config.CACHE_TTL())), '%d-%m-%Y:%H-%M-%S')
            if ttl_live<format(datetime.datetime.now(),'%d-%m-%Y:%H-%M-%S'):
            	self.lock.acquire()
            	del self.dict[file_hash]
            	self.lock.release()
            	return False
            else:
                return True
        else:
            return False


    def mod_get_node_port(self, node_id):
        # To get port number of node id (NOT REQUIRED)



    def mod_get_node_ip(self, node_id):
		#To get IP of node id (NOT REQUIRED)
		print "hi"


	#searching if the file is present in the node
	def mod_file_is_present(self,file_hash):
		# just have to check whether the file_dict(file_hash,file_name) contains the file_hash or not.
		# add map in class
		return self.file_dict.has_key(file_hash)

	# This function invoke from the mod_search if local node does not contain the file
	# returns a list containing list[IP,PORT] of the node contains the file. Otherwise returns list[0,0]
	def mod_rpc_search(self, file_hash):

		if file_hash < self.node_id and self.pred > self.node_id :
			#it is with me
			if self.mod_file_is_present(file_hash):
				actual_ip= self.mod_get_node_ip(self.node_id)
				actual_port= self.mod_get_node_port(self.node_id)

			else:
				actual_ip= 0
				actual_port= 0

			actual_list=[actual_ip,actual_port]

		elif file_hash > self.pred and file_hash < self.node_id:
			#it is with me
			if self.mod_file_is_present(file_hash):
				actual_ip= self.mod_get_node_ip(self.node_id)
				actual_port= self.mod_get_node_port(self.node_id)

			else:
				actual_ip= 0
				actual_port= 0

			actual_list=[actual_ip,actual_port]

		elif file_hash < self.pred:
			remote_proxy1 = xmlrpclib.ServerProxy("http://" + self.mod_get_node_ip(self.pred) + ":" + self.mod_get_node_port(self.pred) + "/")
			actual_list=remote_proxy1.mod_rpc_search(file_hash)

		else:
			remote_proxy1 = xmlrpclib.ServerProxy("http://" + self.mod_get_node_ip(self.succ) + ":" + self.mod_get_node_port(self.succ) + "/")
			actual_list=remote_proxy1.mod_rpc_search(file_hash)

		return actual_list


	# Expects the hash_digest of file name and returns true on successful file download. Otherwise false.
	def mod_search(self, file_hash):

		if self.mod_check_cache_validity(file_hash):
			actual_ip = self.cache.get(file_hash)[0]
			actual_port = self.cache.get(file_hash)[1]
		else:
			if file_hash < self.node_id and self.pred > self.node_id :
				#it is with me
				if self.mod_file_is_present(file_hash):
					actual_ip= self.local_ip
					actual_port= self.local_port

				else:
					actual_ip= 0
					actual_port= 0

			elif file_hash > self.pred and file_hash < self.node_id:
				#it is with me
				if self.mod_file_is_present(file_hash):
					actual_ip= self.mod_get_node_ip(self.node_id)
					actual_port= self.mod_get_node_port(self.node_id)

				else:
					actual_ip= 0
					actual_port= 0

			elif file_hash < self.pred:
				remote_proxy1 = xmlrpclib.ServerProxy("http://" + self.mod_get_node_ip(self.pred) + ":" + self.mod_get_node_port(self.pred) + "/")
				actual_list=remote_proxy1.mod_rpc_search(file_hash)
				actual_ip=actual_list[0]
				actual_port=actual_list[1]

			else:
				remote_proxy1 = xmlrpclib.ServerProxy("http://" + self.mod_get_node_ip(self.succ) + ":" + self.mod_get_node_port(self.succ) + "/")
				actual_list=remote_proxy1.mod_rpc_search(file_hash)
				actual_ip=actual_list[0]
				actual_port=actual_list[1]

		#---------- Downloading File-----------------------------------------------------------
		if actual_ip==0 and actual_port==0:
			return False
		else:
			remote_proxy1 = xmlrpclib.ServerProxy("http://" + actual_ip + ":" + actual_port + "/")
			download_file_size = remote_proxy1.mod_file_download_transfer(file_hash, self.local_port, self.ratio)

		if download_file_size == -1:
			return False

		else:
			self.download_amt = self.download_amt + download_file_size

			self.mod_update_ratio()
			self.mod_cache_update(file_hash, actual_ip, actual_port)

			return True

		#---------------------------------Sourav and Midhun----------------------------------------------




def main():
	# Details of current node
	local_ip = "localhost"
	local_port = sys.argv[1]

	# Creating the local object
	local_node = collab_system(local_ip, local_port)

	# Declared an XMLRPC server
	# This is the listener part of the application
	local_listener = SimpleXMLRPCServer((local_ip, int(local_port)), logRequests = False, allow_none = True)
	
	##print "[Listening on port : %s]" % local_port

	local_listener.register_introspection_functions()
	local_listener.register_multicall_functions()
	local_listener.register_instance(local_node)

	# Initialized the XMLRPC server in a seperate thread
	server_thread = threading.Thread(target = local_listener.serve_forever)

	# Making the server thread a daemon process so that the server dies when the
	# client exits the front end
	server_thread.daemon = True

	# Starting the server thread
	server_thread.start()

	# Details of remote node
	remote_ip = "localhost"
	remote_port = raw_input("\n\tEnter remote port ID : ")

	# Creating connection details of remote node
	remote_proxy = xmlrpclib.ServerProxy("http://" + remote_ip + ":" + remote_port + "/")

	# Creating the directory which will contain all hosted files
	os.makedirs(local_node.dir_hosted)

	# Creating the directory which will contain all downloaded files
	os.makedirs(local_node.dir_downloaded)

	while True:
		os.system('clear')

		print "\n\n\t. : Collab Menu for %s : .\n" % local_port
		print "\tSearch & download      ...[1]"
		print "\tUpload                 ...[2]"
		print "\tAdmin Menu             ...[3]"
		print "\tExit                   ...[0]"

		input_val = raw_input("\n\n\tEnter option : ")

		if input_val == "1":
			file_name = raw_input("\n\tEnter name of file to be downloaded : ")
			file_hash=local_node.mod_hash_string(file_name)

			file_lookup = local_node.mod_search(file_hash, remote_proxy)

			if file_lookup == False:
				print "\n\tFile not found at remote node!"
			else:
				print "\n\tFile downloaded!"

			local_node.return_pause()
			
		elif input_val == "2":
			file_name = raw_input("\n\tEnter name of file to be uploaded : ")

			# Creaating path of local file about to be uploaded
			file_path = "./" + file_name

			if os.path.exists(file_path) == True:
				local_node.mod_file_upload(file_path, file_name, remote_proxy)

				print "\n\tFile uploaded!"
			else:
				print "\n\tFile not found at current node!"

			local_node.return_pause()

		elif input_val == "3":
			while True:
				os.system('clear')

				print "\n\n\t. : Admin Menu for %s : .\n" % local_port
				print "\tSee finger table       ...[1]"
				print "\tSee local files        ...[2]"
				print "\tSee query cache        ...[3]"
				print "\tSee neighbours         ...[4]"
				print "\tSee statistics         ...[5]"
				print "\t<< Back <<             ...[0]"

				admin_inp_val = raw_input("\n\n\tEnter option : ")

				if admin_inp_val == "1":
					local_node.return_pause()

				elif admin_inp_val == "2":

					file_list = local_node.mod_show_files()

					if file_list:
						print "\n\tThe files are...\n"

						for files in file_list:
							print "\t\t%s" % files
					else:
						print "\n\tThe current directory is empty...\n"

					local_node.return_pause()

				elif admin_inp_val == "3":
					local_node.return_pause()

				elif admin_inp_val == "4":
					local_node.return_pause()

				elif admin_inp_val == "5":
					local_node.mod_show_stats()
					local_node.return_pause()

				elif admin_inp_val == "0":
					break

				else:
					print "\tIncorrect option value"
					print "\tTry again..."
					local_node.return_pause()

		elif input_val == "0":
			print "\n\tThank you for using Collab!"
			local_node.return_pause()
			
			break

		else:
			print "\n\tIncorrect option value"
			print "\n\tTry again..."
			local_node.return_pause()

	os.system('clear')

if __name__ == '__main__':
	main()