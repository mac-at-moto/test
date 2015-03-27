import luigi
import os

class UserDataBuild(luigi.Task):

  def requires(self):
    return []

  def output(self):
    return luigi.LocalTarget('/tmp/features.csv')

  def run(self):
    """
    TODO: take sample data and save to local file
    read data from /tmp/inputfinal.csv, extract features,
    save the featues to /tmp/features.csv
    """
    print "XXX finished fetching user data"

class LambdaCheck(luigi.Task):
  executed = False

  def requires(self):
    return [UserDataBuild()]

  def complete(self):
    return self.executed

  def run(self):
    os.remove('/tmp/lambda')
    executed = True
    print "XXX finished checking lambda"

class KmeansCheck(luigi.Task):
  executed = False

  def requires(self):
    return [UserDataBuild(), LambdaBuild()]

  def complete(self):
    return self.executed

  def run(self):
    os.remove('/tmp/kmeans')
    executed = True
    print "XXX finished checking Kmeans model"

class LambdaBuild(luigi.Task):

  def requires(self):
    return [UserDataBuild()]

  def output(self):
    return luigi.LocalTarget('/tmp/lambda')

  def run(self):
    print "XXX input is: ", self.input()
    result = str(0.1)
    with self.output().open('w') as f:
      f.write(result)
    print "XXX finished optimizing Lambda"

class KmeansBuild(luigi.Task):

  def requires(self):
    return [UserDataBuild(), LambdaBuild()]

  def output(self):
    return luigi.LocalTarget('/tmp/kmeans')

  def run(self):
    self.output().open('w').close()
    print "XXX finished building Kmeans model"

class KmeansLabel(luigi.Task):

  def requires(self):
    return [UserDataBuild(), LambdaBuild(), KmeansBuild()]

  def run(self):
    print "XXX finished the labeling task ", os.environ['BUILD_NUMBER']

if __name__ == '__main__':
    luigi.run()
