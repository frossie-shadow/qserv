#!/usr/bin/perl -w

use strict;
use Getopt::Long;
use Cwd;

Getopt::Long::config('bundling_override');
my %opts = ();
GetOptions( \%opts, 
	"debug",
	"help|h",
	"status|s",
	"stop",
	"start",
	"load",
	"delete-data",
	"source=s",
	"table=s",
	"output=s",
	"dbpass=s",
	"mysql-proxy-port=s",
	"partition",
	"test"
);
usage(1) if ($Getopt::Long::error); 
usage(0) if ($opts{'help'});

my $debug = $opts{'debug'} || 0;

my $install_dir = "<QSERV_BASE_DIR>";
my $mysql_proxy_port = "<MYSQL_PROXY_PORT>";

#mysql variables
my $mysqld_sock = "$install_dir/var/lib/mysql/mysql.sock";

if( $opts{'status'} ) {

        unless( $opts{'dbpass'} ) {
                print "Error: you need to specify the mysql root password with the --dbpass option.\n";
                exit(1);
        }

	if( check_mysqld( $opts{'dbpass'}  ) ) {
		print "Mysql server up and running.\n";
	} else {
		print "Mysql server not running.\n";
	}

	if( check_proxy() ) {
		print "Mysql proxy up and running.\n";
	} else {
		print "Mysql proxy not running.\n";
	}

	if( check_xrootd() ) {
		print "Xrootd server up and running.\n";
	} else {
		print "Xrootd server not running.\n";
	}

	if( check_qserv() ) {
		print "Qserv server up and running.\n";
	} else {
		print "Qserv server not running.\n";
	}

} elsif( $opts{'stop'} ) {
	
	print "Stopping mysql-proxy\n";
	stop_proxy();
	print "Stopping xrootd\n";
	stop_xrootd();
	print "Stopping mysqld\n"; 
	stop_mysqld();
	print "Stopping qserv\n"; 
	stop_qserv();
	
} elsif( $opts{'start'} ) {

	start_proxy();
	start_xrootd();
	start_mysqld();
	start_qserv();
	
} elsif( $opts{'partition'} ) {

	

	#need to partition raw data for loading into qserv.
	unless( $opts{'source'} ) {
		print "Error: you need to set the path to the source data with the --source option.\n";
		exit(1);
	}
	unless( $opts{'table'} ) {
		print "Error: you need to specify the table name for the source data with the --table option.\n";
		exit(1);
	}
	unless( $opts{'output'} ) {
		print "Error: you need to specify the output path with the --output option.\n";
		exit(1);
	}
	
	partition_data( $opts{'source'}, $opts{'output'}, $opts{'table'} );
	
} elsif( $opts{'load'} ) {

        #need to partition raw data for loading into qserv.
        unless( $opts{'source'} ) {
                print "Error: you need to set the path to the source data with the --source option.\n";
               	exit(1);
        }
        unless( $opts{'table'} ) {
                print "Error: you need to specify the table name for the source data with the --table option.\n";
               	exit(1);
        }
       	unless( $opts{'output'} ) {
                print "Error: you need to specify the output path with the --output option.\n";
                exit(1);
        }
       unless( $opts{'dbpass'} ) {
                print "Error: you need to specify the mysql root password with the --dbpass option.\n";
                exit(1);
        }

	#need to load data into qserv
	## TODO performs some checking before starting mysqld (already started)
	## and idem for stopping
	load_data( $opts{'source'}, $opts{'output'}, $opts{'table'}, $opts{'dbpass'} );

} elsif( $opts{'delete-data'} ) {

       unless( $opts{'dbpass'} ) {
                print "Error: you need to specify the mysql root password with the --dbpass option.\n";
                exit(1);
        }

	# deleting data into qserv
	delete_data( $opts{'source'}, $opts{'output'}, $opts{'table'}, $opts{'dbpass'} );
}


#############################################################

#Check the sql server status, if it is up or down by using 
#an sql command for input.
sub check_sql_server {
	my( $command ) = @_;

	print "Testing sql with command $command\n" if( $debug );
	
	my $testsql = "/tmp/tmp.sql";
	create_test_sql( $testsql );
	
	#try through the proxy to see if it can talk to mysql server.
	my @reply = run_command("$command < $testsql 2>&1");
	
	print "@reply\n" if( $debug );
	
	if( $reply[0] =~ /Database/ ) {
		return 1;
	} else {
		return 0;
	}
	
	unlink "$testsql";
	
}

#Create a test sql command to test the sql server.
sub create_test_sql {
	my( $testsql ) = @_;
	
	#create tmp sql file
	open TMPFILE, ">$testsql";
	print TMPFILE "show databases;\n";
	close TMPFILE;
	
}

#check the mysql proxy use.
sub check_proxy {

	return check_sql_server( "mysql --port=<XROOTD_PORT> --protocol=TCP" );

}

#check the mysql server status.
sub check_mysqld {

	my( $dbpass ) = @_;
	if( -e "$mysqld_sock" ) {
		return check_sql_server( "mysql -S $mysqld_sock -u root -p$dbpass" );
	} else {
		return 0;
	}
}

#check the xrootd process.
sub check_xrootd {
	
	if( check_ps( "xrootd -c" ) && check_ps( "cmsd -c" ) ) {
		return 1;
	} else {
		return 0;
	}

}

#check the qserv process.
sub check_qserv {
	
	return check_ps( "startQserv" );

}

#Check the existence of a process in the process table by a test 
#string in the command line used.
sub check_ps {
	my( $test_string ) = @_;
	print "Check_ps : $test_string\n";
	
	my @reply = run_command("ps x");
	print "Reply : @reply\n";
	my @stuff = grep /$test_string/, @reply;
	print "Catched stuff : @stuff\n-----\n";
	
	if( @stuff ) {
		my( $pid ) = $stuff[0] =~ /^\s*(\d+) /;
		return $pid;
	} else {
		return 0;
	}
}

#Stop the qserv process
sub stop_qserv {

	stop_ps( "startQserv" );

}

#stop the xrootd process
sub stop_xrootd {

	stop_ps( "xrootd -c" );
	stop_ps( "cmsd -c" );

}

#stop the mysql server
sub stop_mysqld {

	stop_ps( "mysqld --basedir" );

}

#stop the mysql proxy
sub stop_proxy {

	stop_ps( "mysql-proxy" );

}


#Stop a process based on the exsitence of a string in the command line
#used to start the process.
sub stop_ps {
	my( $test_string ) = @_;
	print "Stopping $test_string\n";
	
	my $pid = check_ps( $test_string );
	print "pid to stop -- $pid\n";
	if( $pid ) {
		if( $opts{'test'} ) {
			print "I would now kill process $pid.\n";
		} else {
			run_command("kill $pid");
		}
	}

}

sub start_proxy {

	system("$install_dir/start_mysqlproxy");

}

sub start_mysqld {

	system("$install_dir/bin/mysqld_safe &");

}

sub start_qserv {

	system("$install_dir/start_qserv");

}

sub start_xrootd {

	system("$install_dir/start_xrootd");
	
}

#Partition the pt11 example data for use into chunks.  This is the example
#use of partitioning, and this should be more flexible to create different
#amounts of chunks, but works for now.
sub partition_data {
	my( $source_dir, $output_dir, $tablename ) = @_;
	
	my( $dataname ) = $source_dir =~ m!/([^/]+)$!;
	
	if( -d "$output_dir" ) {
		chdir "$output_dir";
	} else {
		print "Error: the output dir $output_dir doesn't exist.\n";
	}

	#need to have the various steps to partition data
	my $command = "$install_dir/bin/python $install_dir/qserv/master/examples/partition.py ".
		"-P$tablename -t 2 -p 4 $source_dir/${tablename}.txt -S 10 -s 2";
	if( $opts{'test'} ) {
		print "$command\n";
	} else {
		run_command("$command");
	}
}

sub delete_data {
	my( $source_dir, $location, $tablename, $dbpass ) = @_;
        
        #delete database
        run_command("$install_dir/bin/mysql -S '$install_dir/var/lib/mysql/mysql.sock\' -u root -p'$dbpass' -e 'Drop database if exists LSST;'");
}

#load the partitioned pt11 data into qserv for use.  This does a number of
#steps all in one command.
sub load_data {
	my( $source_dir, $location, $tablename, $dbpass ) = @_;
	
	#create database if it doesn't exist
	run_command("$install_dir/bin/mysql -S '$install_dir/var/lib/mysql/mysql.sock' -u root -p'$dbpass' -e 'Create database if not exists LSST;'");
        
        # qservMeta database creation
        run_command("$install_dir/bin/mysql -S '$install_dir/var/lib/mysql/mysql.sock' -u root -p'$dbpass' < '$install_dir/tmp/qservmeta.sql'");
	
	#check on the table def, and add need columns
	print "Copy and changing $source_dir/${tablename}.sql\n";
	my $tmptable = lc $tablename;
	run_command("cp $source_dir/${tablename}.sql $install_dir/tmp");
	run_command("perl -pi -e 's,^.*_chunkId.*\n,,' $install_dir/tmp/${tablename}.sql");
	run_command("perl -pi -e 's,^.*_subChunkId.*\n,,' $install_dir/tmp/${tablename}.sql");
	run_command("perl -pi -e 's!^\(\\s*PRIMARY\)!  chunkId int\(11\) default NULL,\\n\\1!' $install_dir/tmp/${tablename}.sql");
	run_command("perl -pi -e 's!^\(\\s*PRIMARY\)!  subChunkId int\(11\) default NULL,\\n\\1!' $install_dir/tmp/${tablename}.sql");
	run_command("perl -pi -e 's!^\\s*PRIMARY KEY\\s+\\(.*\\)!  PRIMARY KEY \(${tmptable}Id, subChunkId\)!' $install_dir/tmp/${tablename}.sql");

	#regress through looking for partitioned data, create loading script
	open LOAD, ">$install_dir/tmp/${tablename}_load.sql";
	
	my %chunkslist = ();
	opendir DIR, "$location";
	my @dirslist = readdir DIR;
	closedir DIR;
	
	#look for paritioned table chunks, and create the load data sqls.
	foreach my $dir ( @dirslist ) {
		next if( $dir =~ /^\./ );
		
		if( $dir =~ /^stripe/ ) {
			opendir DIR, "$location/$dir";
			my @filelist = readdir DIR;
			closedir DIR;
			
			foreach my $file ( @filelist ) {
				if( $file =~ /(\w+)_(\d+).csv/ ) {
					if( $1 eq $tablename ) {
					print LOAD "CREATE TABLE IF NOT EXISTS ${1}_$2 LIKE $tablename;\n";
					print LOAD "LOAD DATA INFILE '$location/$dir/$file' INTO TABLE ${1}_$2 FIELDS TERMINATED BY ',';\n";

					$chunkslist{$2} = 1;
					}
				}
			}
		}
	}
	print LOAD "CREATE TABLE IF NOT EXISTS ${tablename}_1234567890 LIKE $tablename;\n";
	close LOAD;
	
	#load the data into the mysql instance
	print "Loading data, this make take awhile...\n";
	run_command("$install_dir/bin/mysql -S '$install_dir/var/lib/mysql/mysql.sock' -u root -p'$dbpass' LSST < '$install_dir/tmp/${tablename}.sql'");
	run_command("$install_dir/bin/mysql -S '$install_dir/var/lib/mysql/mysql.sock' -u root -p'$dbpass' LSST < '$install_dir/tmp/${tablename}_load.sql'");
		
	#create the empty chunks file
	create_emptychunks( \%chunkslist );
	
	#create a setup file
	unless( -e "$install_dir/etc/setup.cnf" ) {
		open SETUP, ">$install_dir/etc/setup.cnf";
		print SETUP "host:localhost\n";
		print SETUP "port:$mysql_proxy_port\n";
		print SETUP "user:root\n";
		print SETUP "pass:$dbpass\n";
		print SETUP "sock:$install_dir/var/lib/mysql/mysql.sock\n";
		close SETUP;
	}

	#regester the database, export
	my $command = "$install_dir/qserv/worker/tools/qsDbTool ";
	$command .= "-a $install_dir/etc/setup.cnf -i 1 ";
	$command .= "register LSST Object";
	print "Command: $command\n";
	run_command("$command");
	
	$command = "$install_dir/qserv/worker/tools/qsDbTool ";
	$command .= "-a $install_dir/etc/setup.cnf -i 1 -b $install_dir/xrootd-run ";
	$command .= "export LSST";
        print "Command: $command\n";
	run_command("$command");
	
}

#Create the empty chucks list up to 1000, and print this into the
#empty chunks file in etc.
sub create_emptychunks {
	my( $chunkslist ) = @_;
		
	open CHUNKS, ">$install_dir/etc/emptyChunks.txt";
	for( my $i = 0; $i < 1000; $i++ ) {
		unless( defined $chunkslist->{$i} ) {
			print CHUNKS "$i\n";
		}
	}
	close CHUNKS;
	
}

#help report for the --help option
sub usage {
  my($exit, $message) = @_;

        my($bin) = ($0 =~ m!([^/]+)$!);
        print STDERR $message if defined $message;
        print STDERR <<INLINE_LITERAL_TEXT;     
usage: $bin [options]
  Help admin the qserv server install, starting, stopping, and checking of status
  Also supports the loading of pt11 example data into the server for use.

Options are:
      --debug        Print out debug messages.
  -h, --help         Print out this help message.
  -s, --status       Print out the status of the server processes.
      --stop         Stop the servers.
      --start        Start the servers.
      --load         Load data into qserv, requires options source, output, table
      --delete-data  Load data into qserv, requires options source, output, table
      --source       Path to the pt11 exmple data
      --output       Path to the paritioned data
      --table        Table name for partitioning and loading
      --partition    Partition the example pt11 data into chunks, requires source, output, table
      --test         Test the use of the util, without performing the actions.

Examples: $bin --status

Comments to Douglas Smith <douglas\@slac.stanford.edu>.
INLINE_LITERAL_TEXT

	       exit($exit) if defined $exit;

}

sub run_command 
{
        my( $command ) = @_;
        my $cwd = cwd();
	my @return;
        print "-- Running: $command in $cwd\n";
        open(OUT, "$command 2>&1 |") || die "ERROR : can't fork $command : $!";
        while (<OUT>) 
        {
                print STDOUT $_; 
                push (@return, $_); 
        }
        close(OUT) || die "ERROR : $command exits with error code ($?)";
	return @return;
}
