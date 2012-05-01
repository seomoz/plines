# Add Redis::Set#move from redis-objects master branch. Don't want to
# switch to a :git gem just for this. Hopefully it'll be released w/
# this soon.
require 'redis/objects'
require 'redis/set'

Redis::Set.class_eval do
  # Moves value from one set to another. Destination can be a String
  # or Redis::Set.
  #
  #   set.move(value, "name_of_key_in_redis")
  #   set.move(value, set2)
  #
  # Returns true if moved successfully.
  #
  # Redis: SMOVE
  def move(value, destination)
    redis.smove(key,
      destination.is_a?(Redis::Set) ? destination.key : destination.to_s,
      value)
  end unless instance_method(:move).arity == 2
  # All redis objects have a #move method that takes 1 argument; it moves the
  # entire object to a new redis key. #move here takes 2 args and moves one
  # entry in the set to a different set.
end
