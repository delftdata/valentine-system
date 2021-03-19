from flask_wtf import FlaskForm
from wtforms import FileField
from wtforms.validators import DataRequired


class BaseForm(FlaskForm):
    class Meta:
        csrf = False


class UploadFileToMinioForm(BaseForm):
    resource = FileField('file', validators=[DataRequired()])
