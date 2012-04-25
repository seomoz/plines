# Project-specific configuration for CruiseControl.rb
Project.configure do |project|
  # Send email notifications about broken and fixed builds (default: send to nobody)
  project.email_notifier.emails = ['turbo@seomoz.org']

  # Set email 'from' field:
  project.email_notifier.from = 'seomoz@cruise.corp.seomoz.org'

  # Build the project by invoking rake task 'custom'
  # project.rake_task = 'cc'

  # Build the project by invoking shell script "build_my_app.sh". Keep in mind that when the script is invoked,
  # current working directory is <em>[cruise data]</em>/projects/your_project/work, so if you do not keep build_my_app.sh
  # in version control, it should be '../build_my_app.sh' instead
  project.build_command = 'script/cruise_build'

  # Ping source control for new revisions every 5 minutes (default: 30 seconds)
  # project.scheduler.polling_interval = 5.minutes

  #project.campfire_notifier.account = 'seomoz'
  #project.campfire_notifier.token   = '31283b97da099c10ca3bb57529d5a242a2f233a1'
  #project.campfire_notifier.room    = 'Tools'
  #project.campfire_notifier.ssl     = true
end
